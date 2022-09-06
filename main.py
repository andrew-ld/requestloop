import argparse
import json
import logging
import sys
import traceback

import aiohttp.web
import asyncio
import uvloop


__all__ = ("RequestLoop",)

LOGGER = logging.getLogger("rl")
LOGGER.setLevel(logging.DEBUG)
LOGGER.addHandler(logging.StreamHandler(sys.stdout))


class RequestLoopTask:
    __slots__ = ("_req_method", "_req_body", "_req_headers", "_req_url", "_callback_url")

    _req_method: str
    _req_body: str
    _req_headers: dict[str, str]
    _req_url: str
    _callback_url: str

    def __init__(self, req_method: str, req_body: str, req_headers: dict[str, str], req_url: str, callback_url: str):
        self._req_method = req_method
        self._req_body = req_body
        self._req_headers = req_headers
        self._req_url = req_url
        self._callback_url = callback_url

    @property
    def req_method(self) -> str:
        return self._req_method

    @property
    def req_body(self) -> str:
        return self._req_body

    @property
    def req_headers(self) -> dict[str, str]:
        return self._req_headers

    @property
    def req_url(self) -> str:
        return self._req_url

    @property
    def callback_url(self) -> str:
        return self._callback_url


class RequestLoopReqData:
    __slots__ = ("_status", "_body", "_headers")

    _status: int
    _body: str
    _headers: dict[str, str]

    def __init__(self, status: int, body: str, headers: dict[str, str]):
        self._status = status
        self._body = body
        self._headers = headers

    def to_post_data(self) -> dict[str, ...]:
        return dict(status=self._status, body=self._body, headers=json.dumps(self._headers), ok=True)


class RequestLoop:
    __slots__ = ("_listen_host", "_listen_port", "_tasks", "_http_client_session")

    _listen_host: str
    _listen_port: int
    _tasks: asyncio.Queue[RequestLoopTask]
    _http_client_session: aiohttp.ClientSession

    def __init__(self, listen_host: str, listen_port: int):
        self._listen_host = listen_host
        self._listen_port = listen_port
        self._tasks = asyncio.Queue()
        self._http_client_session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(), connector_owner=True)

    async def listen_http_server(self):
        application = aiohttp.web.Application()
        application.add_routes([aiohttp.web.post("/enqueue", self._http_endpoint_enqueue)])
        application.add_routes([aiohttp.web.get("/ping", self._http_endpoint_healthcheck)])
        await aiohttp.web._run_app(application, host=self._listen_host, port=self._listen_port)

    async def consume_tasks(self):
        while True:
            task = await self._tasks.get()

            try:
                task_data = await self._fetch_task_req_data(task)
            except:
                task_data = None
                traceback.print_exc()

            if task_data:
                try:
                    await self._post_task_data_to_callback(task, task_data)
                except:
                    traceback.print_exc()
            else:
                try:
                    await self._post_task_error_to_callback(task)
                except:
                    traceback.print_exc()

    async def _http_endpoint_healthcheck(self, _: aiohttp.web.Response) -> aiohttp.web.Response:
        return aiohttp.web.Response(status=200, text="pong")

    async def _http_endpoint_enqueue(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        post_parameters = await request.post()

        req_method = post_parameters.get("req_method")
        req_body = post_parameters.get("req_body")
        req_parameters = json.loads(post_parameters.get("req_headers"))
        req_url = post_parameters.get("req_url")
        callback_url = post_parameters.get("callback_url")

        task = RequestLoopTask(req_method, req_body, req_parameters, req_url, callback_url)

        await self._tasks.put(task)

        return aiohttp.web.Response(status=200, text="ok")

    async def _post_task_data_to_callback(self, task: RequestLoopTask, data: RequestLoopReqData):
        callback_request = self._http_client_session.post(task.callback_url, data=data.to_post_data())

        async with callback_request as callback_response:
            LOGGER.debug("callback post ok %s -> %s, status: %d", task.req_url, task.callback_url, callback_response.status)

    async def _post_task_error_to_callback(self, task: RequestLoopTask):
        callback_request = self._http_client_session.post(task.callback_url, data=dict(ok=False))

        async with callback_request as callback_response:
            LOGGER.debug("callback post failure %s -> %s, status: %d", task.req_url, task.callback_url, callback_response.status)

    async def _fetch_task_req_data(self, task: RequestLoopTask) -> RequestLoopReqData:
        upstream_request = self._http_client_session.request(method=task.req_method, url=task.req_url, headers=task.req_headers)

        async with upstream_request as upstream_response:
            status = upstream_response.status
            body = await upstream_response.text()
            headers = dict(upstream_response.headers.items())

            LOGGER.debug("upstream fetch %s, status: %d", task.req_url, status)

            return RequestLoopReqData(status, body, headers)


async def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-listen-host", type=str, required=True)
    argparser.add_argument("-listen-port", type=int, required=True)
    argparser.add_argument("-parallelism", type=int, required=True, default=128)

    arguments = argparser.parse_args()
    app = RequestLoop(arguments.listen_host, arguments.listen_port)

    await asyncio.gather(app.listen_http_server(), *(app.consume_tasks() for _ in range(arguments.parallelism)))


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
