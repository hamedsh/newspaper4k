"""Async HTTP helpers for newspaper4k using httpx."""

import inspect
import logging
from collections.abc import Callable

import httpx
from w3lib.encoding import html_to_unicode

from newspaper import parsers
from newspaper.configuration import Configuration
from newspaper.exceptions import ArticleBinaryDataException, ArticleException, RobotsException
from newspaper.network_hooks import HookableEvent, get_hooks

DEFAULT_ENCODING = "utf-8"

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------

_async_client: httpx.AsyncClient | None = None


def _make_client(config: Configuration | None = None) -> httpx.AsyncClient:
    """Create an httpx.AsyncClient with sensible defaults."""
    cfg = config or Configuration()
    headers = dict(cfg.requests_params.get("headers", {}))
    headers.setdefault("Accept-Encoding", "gzip, deflate, br")

    proxies = cfg.requests_params.get("proxies") or {}
    # httpx accepts either a mapping in `proxies` or a single URL string. Use the
    # mapping when available, otherwise fall back to None.
    proxies_param = proxies or None

    # Pass through TLS/verify settings when provided in requests_params.
    verify_param = cfg.requests_params.get("verify", True)

    return httpx.AsyncClient(
        headers=headers,
        follow_redirects=True,
        max_redirects=10,
        proxies=proxies_param,
        verify=verify_param,
    )


async def get_async_client() -> httpx.AsyncClient:
    """Return a module-level shared async client (lazy init)."""
    global _async_client  # pylint: disable=global-statement
    if _async_client is None or getattr(_async_client, "is_closed", False):
        _async_client = _make_client()
    return _async_client


async def reset_async_client() -> httpx.AsyncClient:
    """Close and recreate the module-level async client."""
    global _async_client  # pylint: disable=global-statement
    if _async_client is not None and not getattr(_async_client, "is_closed", False):
        await _async_client.aclose()
    _async_client = _make_client()
    return _async_client


# ---------------------------------------------------------------------------
# Core request helpers
# ---------------------------------------------------------------------------


async def _run_before_request_hooks(url: str, config: Configuration, method: str = "get", data: str | None = None) -> bool:
    """Run BEFORE_REQUEST hooks; return False if any hook denies the request.

    Supports both sync and async hook callables.
    """
    continue_request = True
    for hook in get_hooks(HookableEvent.BEFORE_REQUEST):
        try:
            if inspect.iscoroutinefunction(hook):
                result = await hook(url, config, method, data)
            else:
                result = hook(url, config, method, data)
        except TypeError:
            # Fallback for older hook signatures that only accept (url, config)
            if inspect.iscoroutinefunction(hook):
                result = await hook(url, config)
            else:
                result = hook(url, config)
        if result is False:
            continue_request = False
    return continue_request


def _html_from_response(response: httpx.Response, config: Configuration) -> str:
    """Extract and decode HTML from an httpx.Response."""
    content_type = response.headers.get("content-type", "")

    if content_type in config.ignored_content_types_defaults:
        return config.ignored_content_types_defaults[content_type]

    _, html = html_to_unicode(
        content_type_header=content_type or None,
        html_body_str=response.content,
        default_encoding=DEFAULT_ENCODING,
    )
    return html or ""


async def do_request_async(
    url: str,
    config: Configuration,
    method: str = "get",
    data: str | None = None,
) -> httpx.Response:
    """Perform an async HTTP request.

    Args:
        url: The URL to request.
        config: Newspaper configuration object.
        method: HTTP method ('get' or 'post').
        data: Optional request body for POST requests.

    Returns:
        httpx.Response

    Raises:
        ArticleBinaryDataException: If binary content is detected and
            ``config.allow_binary_content`` is False.
        RobotsException: If a BEFORE_REQUEST hook blocks the request.
        NotImplementedError: If an unsupported HTTP method is provided.
    """
    # Run before-request hooks (e.g. robots.txt checker)
    if not await _run_before_request_hooks(url, config, method, data):
        raise RobotsException(f"Request to {url} blocked by a before_request hook")

    req_params: dict = {
        "timeout": config.requests_params.get("timeout", 7),
    }
    if config.requests_params.get("headers"):
        req_params["headers"] = config.requests_params["headers"]
    if config.requests_params.get("cookies"):
        req_params["cookies"] = config.requests_params["cookies"]
    if config.requests_params.get("auth"):
        req_params["auth"] = config.requests_params["auth"]
    if "verify" in config.requests_params:
        req_params["verify"] = config.requests_params.get("verify")

    client = await get_async_client()

    try:
        if method == "get":
            response = await client.get(url, **req_params)
        elif method == "post":
            response = await client.post(url, data=data, **req_params)
        else:
            raise NotImplementedError(f"Method {method} not implemented")
    except httpx.RequestError as exc:
        # Call on_error hooks similar to the sync path, then re-raise.
        for hook in get_hooks(HookableEvent.ON_ERROR):
            try:
                if inspect.iscoroutinefunction(hook):
                    await hook(url, config, method, data)
                else:
                    hook(url, config, method, data)
            except TypeError:
                # Support older hook signatures
                if inspect.iscoroutinefunction(hook):
                    await hook(url, config)
                else:
                    hook(url, config)
        # Re-raise the original httpx exception
        raise

    # Run after-response hooks (support async hooks). Keep the same signature
    # as sync code: (url, config, method, data) to be compatible with existing hooks.
    for hook in get_hooks(HookableEvent.AFTER_RESPONSE):
        try:
            if inspect.iscoroutinefunction(hook):
                await hook(url, config, method, data)
            else:
                hook(url, config, method, data)
        except TypeError:
            # Fallback for simpler hook signatures
            if inspect.iscoroutinefunction(hook):
                await hook(url, config)
            else:
                hook(url, config)

    return response


async def get_html_async(
    url: str,
    config: Configuration | None = None,
    response: httpx.Response | None = None,
) -> str:
    """Async version of :func:`newspaper.network.get_html`.

    Returns the HTML content for *url*.  If *response* is provided the
    download is skipped and the HTML is extracted from the cached response.

    Args:
        url: Target URL.
        config: Newspaper configuration object.
        response: Optional pre-fetched httpx.Response.

    Returns:
        str: HTML content, or an empty string on error.
    """
    config = config or Configuration()
    try:
        html, status_code, _ = await get_html_status_async(url, config, response)
        if status_code >= 400:
            log.warning("get_html_async() bad status code %s on URL: %s", status_code, url)
            if config.http_success_only:
                raise ArticleException(f"Http error when downloading {url}. Status code: {status_code}")
            return ""
    except httpx.RequestError as e:
        log.debug("get_html_async() error. %s on URL: %s", e, url)
        return ""

    return html


async def get_html_status_async(
    url: str,
    config: Configuration | None = None,
    response: httpx.Response | None = None,
) -> tuple[str, int, list[httpx.Response]]:
    """Async version of :func:`newspaper.network.get_html_status`.

    Returns a tuple of (html, status_code, redirect_history).

    Args:
        url: Target URL.
        config: Newspaper configuration object.
        response: Optional pre-fetched httpx.Response.

    Returns:
        tuple[str, int, list[httpx.Response]]:
            HTML string, HTTP status code, redirect history list.
    """
    config = config or Configuration()

    if response is not None:
        html = _html_from_response(response, config)
        return html, response.status_code, list(response.history)

    response = await do_request_async(url, config)

    if response.status_code != 200:
        log.warning(
            "get_html_status_async(): bad status code %s on URL: %s",
            response.status_code,
            url,
        )

    html = _html_from_response(response, config)
    if isinstance(html, bytes):
        html = parsers.get_unicode_html(html)

    return html, response.status_code, list(response.history)


async def multithread_request_async(
    urls: list[str],
    config: Configuration | None = None,
) -> list[httpx.Response | None]:
    """Concurrently fetch multiple URLs using asyncio.

    This is the async replacement for
    :func:`newspaper.network.multithread_request`.  All requests are issued
    concurrently via ``asyncio.gather``.

    Args:
        urls: List of URLs to fetch.
        config: Newspaper configuration object.

    Returns:
        list[httpx.Response | None]:
            Responses in the same order as *urls*; ``None`` for failures.
    """
    import asyncio  # noqa: PLC0415

    config = config or Configuration()

    async def _fetch(url: str) -> httpx.Response | None:
        try:
            return await do_request_async(url, config)
        except httpx.TimeoutException:
            log.error("multithread_request_async(): Timeout for URL: %s", url)
            return None
        except httpx.RequestError as e:
            log.warning("multithread_request_async(): Http error %s on URL: %s", e, url)
            return None
        except RobotsException as e:
            log.warning("multithread_request_async(): Robots.txt blocked URL: %s. %s", url, e)
            return None

    results = await asyncio.gather(*[_fetch(u) for u in urls])
    return list(results)
