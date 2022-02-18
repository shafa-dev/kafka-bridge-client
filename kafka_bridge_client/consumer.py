import asyncio
import json
import typing as t
from urllib.parse import urljoin
import signal
from xxlimited import Str

import aiohttp

from . import exceptions


class Response(t.NamedTuple):
    content: bytes
    status: int


def _exit_handler(
    sig_num: 'signal.Signals',
    flag: t.List[bool],
    consumer: 'KafkaBridgeConsumer'
) -> None:
    if flag:
        raise SystemExit(128 + sig_num)
    else:
        loop = asyncio.get_running_loop()
        loop.run_until_complete(consumer.delete_consumer_instance())
        flag.append(True)


def _graceful_exit(consumer: 'KafkaBridgeConsumer') -> None:
    flag: t.List[bool] = []
    loop = asyncio.get_running_loop()
    for sig_num in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_num, _exit_handler, sig_num, consumer, flag)


class KafkaBridgeConsumer:
    """
    Docs: https://strimzi.io/docs/bridge/latest
    Example: https://strimzi.io/blog/2019/11/05/exposing-http-bridge/
    """
    TOPICS_PATH = 'topics'
    CONSUMER_PATH = 'consumers/{group_id}'
    CONSUMER_INSTANCE_PATH = 'consumers/{group_id}/instances/{name}'
    SUBSCRIPTION_PATH = 'consumers/{group_id}/instances/{name}/subscription'
    RECORDS_PATH = 'consumers/{group_id}/instances/{name}/records'
    COMMIT_PATH = 'consumers/{group_id}/instances/{name}/offsets'

    def __init__(
        self,
        *topics: str,
        bootstrap_server: str,
        group_id: str,
        auto_offset_reset: str,
        # auto_offset_reset: t.Literal['earliest', 'latest'],
        enable_auto_commit: bool,
        consumer_name: str,
        sleep_interval_seconds: int = 2,
        client_timeout_seconds: int = 15,
        headers: t.Dict[str, t.Any] = None,
        proxy: str = 'strimzi',
        # proxy: t.Literal['strimzi', 'confluent'] = 'strimzi',
        content_type: str = 'application/vnd.kafka.v2+json'
    ) -> None:
        self._content_type = content_type
        self._group_id = group_id
        self._consumer_name = consumer_name
        self._topics = topics
        self._offsets: t.Dict[str, t.Dict[str, t.Any]] = {}
        self._proxy = proxy
        if proxy == 'strimzi':
            self._config = {
                'auto.offset.reset': auto_offset_reset,
                'enable.auto.commit': enable_auto_commit,
                'format': 'binary',
                'name': consumer_name,
            }
        elif proxy == 'confluent':
            self._config = {
                'auto.offset.reset': auto_offset_reset,
                'auto.commit.enable': 'true' if enable_auto_commit else 'false',
                'format': 'binary',
                'name': consumer_name,
            }

        # Delay between fetching of records if
        # the previous fetch return zero records
        self._sleep_interval_seconds = sleep_interval_seconds
        self._bootstrap_server = bootstrap_server
        self._client_timeout_seconds = client_timeout_seconds
        self._headers = headers or {}

    @property
    def is_strimzi_proxy(self) -> bool:
        return self._proxy == 'strimzi'

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: t.Dict[str, t.Any] = None,
        data: t.Dict[str, t.Any] = None,
        headers: t.Dict[str, t.Any] = None,
    ) -> Response:
        params = params or {}
        data = data or {}
        _headers = headers or {}
        _headers.update(self._headers)
        _headers['Content-Type'] = self._content_type
        url = urljoin(self._bootstrap_server, path)

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(self._client_timeout_seconds),
            headers=_headers,
        ) as session:
            try:
                resp = await session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=data,
                )
            except aiohttp.ClientError as e:
                raise exceptions.KafkaBridgeClientError(e)
            except asyncio.TimeoutError as e:
                raise exceptions.KafkaBridgeTimeoutError(e)
            else:
                content = await resp.read()
                return Response(content=content, status=resp.status)

    async def __aenter__(self) -> None:
        try:
            # if process killed by OOM we cant delete consumer
            # so we need try delete consumer before creating new consumer
            await self.delete_consumer_instance()
        except exceptions.ConsumerNotFound:
            pass

        await self._create_consumer_instance()
        try:
            await self._subscribe()
        except exceptions.KafkaBridgeError:
            await self.delete_consumer_instance()
            raise

        _graceful_exit(self)

        return None

    async def __aexit__(self, *args: t.Any, **kwargs: t.Any) -> None:
        await self.delete_consumer_instance()

    async def _create_consumer_instance(self) -> None:
        response = await self._request(
            'POST',
            self.CONSUMER_PATH.format(group_id=self._group_id),
            data=self._config,
        )
        if response.status != 200:
            raise exceptions.KafkaBridgeError(
                f'status: {response.status}, text: {response.content!r}',
            )

    async def delete_consumer_instance(self) -> None:
        if not self._consumer_name:
            return None

        response = await self._request(
            'DELETE',
            self.CONSUMER_INSTANCE_PATH.format(
                group_id=self._group_id,
                name=self._consumer_name,
            ),
        )
        if response.status != 204:
            if response.status == 404:
                raise exceptions.ConsumerNotFound
            else:
                raise exceptions.KafkaBridgeError(
                    f'status: {response.status}, text: {response.content!r}',
                )

    async def _subscribe(self) -> None:
        response = await self._request(
            'POST',
            self.SUBSCRIPTION_PATH.format(
                group_id=self._group_id,
                name=self._consumer_name,
            ),
            data={'topics': self._topics},
        )

        if response.status != 204:
            raise exceptions.KafkaBridgeError(
                f'status: {response.status}, text: {response.content!r}',
            )

    async def get_records(
        self,
        *,
        max_bytes: int,
        timeout: int = 10000
    ) -> t.AsyncGenerator[t.Dict[str, t.Any], None]:
        while True:
            response = await self._request(
                'GET',
                self.RECORDS_PATH.format(
                    group_id=self._group_id,
                    name=self._consumer_name,
                ),
                params={'timeout': timeout, 'max_bytes': max_bytes},
                headers={'Accept': 'application/vnd.kafka.binary.v2+json'},
            )
            if response.status != 200:
                raise exceptions.KafkaBridgeError(
                    f'status: {response.status}, text: {response.content!r}',
                )
            records = json.loads(response.content)

            for rec in records:
                self._offsets[rec['topic']] = {
                    'topic': rec['topic'],
                    'partition': rec['partition'],
                    'offset': rec['offset']
                }
                yield rec

            if not records:
                await asyncio.sleep(self._sleep_interval_seconds)

    async def commit(self) -> None:
        response = await self._request(
            'POST',
            self.COMMIT_PATH.format(
                group_id=self._group_id,
                name=self._consumer_name,
            ),
            data={'offsets': list(self._offsets.values())},
        )
        success_status = 204 if self.is_strimzi_proxy else 200
        if response.status != success_status:
            raise exceptions.KafkaBridgeError(
                f'status: {response.status}, text: {response.content!r}',
            )
