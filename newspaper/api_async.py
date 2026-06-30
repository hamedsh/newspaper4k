"""Async top-level API for newspaper4k.

Mirrors :mod:`newspaper.api` with async equivalents for functions that
perform network I/O.
"""

from newspaper.article import Article
from newspaper.configuration import Configuration
from newspaper.source import Source


async def async_article(url: str, language: str | None = None, **kwargs) -> Article:
    """Async version of :func:`newspaper.article`.

    Downloads and parses an article at *url* without blocking the event loop.

    Args:
        url (str): The URL of the article to download and parse.
        language (str, optional): ISO-639-1 two-letter language code.
        input_html (str, optional): Pre-fetched HTML; skips the network
            request when supplied.
        **kwargs: Additional keyword arguments forwarded to the
            :class:`~newspaper.Article` constructor (or its
            :class:`~newspaper.Configuration`).

    Returns:
        Article: The downloaded and parsed article.

    Example::

        import asyncio
        import newspaper

        async def main():
            article = await newspaper.async_article("https://example.com/news/story")
            print(article.title)

        asyncio.run(main())
    """
    input_html = kwargs.pop("input_html", None)
    a = Article(url, language=language, **kwargs)
    await a.download_async(input_html=input_html)
    a.parse()
    return a


async def async_build(
    url: str = "",
    dry: bool = False,
    only_homepage: bool = False,
    only_in_path: bool = False,
    input_html: str | None = None,
    config: Configuration | None = None,
    **kwargs,
) -> Source:
    """Async version of :func:`newspaper.build`.

    Constructs a :class:`~newspaper.Source` object, downloading and parsing
    the news website homepage and its categories/feeds without blocking the
    event loop.

    Args:
        url (str): URL of the news source homepage (e.g. ``"https://cnn.com"``).
        dry (bool): If True, construct but do not download/parse. Defaults to False.
        only_homepage (bool): Parse only the homepage. Defaults to False.
        only_in_path (bool): Restrict discovered articles to those whose URL
            path is under the source's path. Defaults to False.
        input_html (str, optional): Cached HTML for the homepage.
        config (Configuration, optional): Configuration object.
        **kwargs: Extra configuration options forwarded to
            :class:`~newspaper.Configuration`.

    Returns:
        Source: Constructed :class:`~newspaper.Source` object.

    Example::

        import asyncio
        import newspaper

        async def main():
            source = await newspaper.async_build("https://cnn.com")
            articles = await source.download_articles_async()
            for a in articles[:5]:
                a.parse()
                print(a.title)

        asyncio.run(main())
    """
    config = config or Configuration()
    config.update(**kwargs)
    s = Source(url or "", config=config)
    if not dry:
        await s.build_async(
            only_homepage=only_homepage,
            only_in_path=only_in_path,
            input_html=input_html,
        )
    return s
