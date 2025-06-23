import importlib
import pkgutil
import streaming.plugins


def iter_namespace(ns_pkg):
    # Specifying the second argument (prefix) to iter_modules makes the
    # returned name an absolute name instead of a relative one. This allows
    # import_module to work without having to do additional modification to
    # the name.
    return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")


discovered_plugins = {
    name.split(".")[-1]: importlib.import_module(name)
    for finder, name, ispkg in iter_namespace(streaming.plugins)
}

Stream = lambda type, **kwargs: getattr(discovered_plugins[type], type.capitalize())(
    **kwargs
)
