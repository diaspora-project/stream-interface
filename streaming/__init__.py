import importlib
import pkgutil
import tomllib
import streaming.plugins

from streaming.base import BaseStream


def iter_namespace(ns_pkg):
    # Specifying the second argument (prefix) to iter_modules makes the
    # returned name an absolute name instead of a relative one. This allows
    # import_module to work without having to do additional modification to
    # the name.
    return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")


def from_config(fn: str) -> BaseStream:
    with open(fn, "rb") as f:
        config = tomllib.load(f)

    s = Stream(config["base"]["stream_type"])

    if "producer" in config:
        s.create_producer(**config["producer"])

    if "consumer" in config:
        s.create_consumer(**config["consumer"])

    return s


discovered_plugins = {
    name.split(".")[-1]: importlib.import_module(name)
    for finder, name, ispkg in iter_namespace(streaming.plugins)
}

Stream = lambda stream_type, **kwargs: getattr(
    discovered_plugins[stream_type], stream_type.capitalize()
)(**kwargs)
