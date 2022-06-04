"""
Yangson Data Validator
**********************

Yangson Data Validator on Yangson library for data instance validation using YANG models.

.. autofunction:: yanpd.yang_validator.yang_validator
"""
import logging
import os
import json

log = logging.getLogger(__name__)

try:
    from yangson.statement import ModuleParser
    from yangson import DataModel, enumerations

    HAS_LIBS = True
except ImportError:
    log.debug(
        "yanpd:yang_validator: failed to import Yangson library, make sure it is installed"
    )
    HAS_LIBS = False


def _module_entry(yfile, modmap, submodmap):
    """
    Add entry for one file containing YANG module text.

    :param yfile: (file) File containing a YANG module or submodule.
    """
    data_kws = [
        "augment",
        "container",
        "leaf",
        "leaf-list",
        "list",
        "rpc",
        "notification",
        "identity",
    ]  # Keywords of statements that contribute nodes to the schema tree
    ytxt = yfile.read()
    mp = ModuleParser(ytxt)
    mst = mp.statement()
    submod = mst.keyword == "submodule"
    import_only = True
    rev = ""
    features = []
    includes = []
    rec = {}
    for sst in mst.substatements:
        if not rev and sst.keyword == "revision":
            rev = sst.argument
        elif import_only and sst.keyword in data_kws:
            import_only = False
        elif sst.keyword == "feature":
            features.append(sst.argument)
        elif submod:
            continue
        elif sst.keyword == "namespace":
            rec["namespace"] = sst.argument
        elif sst.keyword == "include":
            rd = sst.find1("revision-date")
            includes.append((sst.argument, rd.argument if rd else None))
    rec["import-only"] = import_only
    rec["features"] = features
    if submod:
        rec["revision"] = rev
        submodmap[mst.argument] = rec
    else:
        rec["includes"] = includes
        modmap[(mst.argument, rev)] = rec


def _make_library(ydir):
    """
    Make JSON library of YANG modules.

    :param ydir: (str) Name of the directory with YANG (sub)modules.
    """
    modmap = {}  # Dictionary for collecting module data
    submodmap = {}  # Dictionary for collecting submodule data
    for infile in os.listdir(ydir):
        if not infile.endswith(".yang"):
            continue
        with open(
            "{ydir}/{infile}".format(ydir=ydir, infile=infile), "r", encoding="utf-8"
        ) as yf:
            _module_entry(yf, modmap, submodmap)
    marr = []
    for item in modmap:
        yam, mrev = item
        men = {"name": yam, "revision": mrev}
        sarr = []
        mrec = modmap[(yam, mrev)]
        men["namespace"] = mrec["namespace"]
        fts = mrec["features"]
        imp_only = mrec["import-only"]
        for (subm, srev) in mrec["includes"]:
            sen = {"name": subm}
            try:
                srec = submodmap[subm]
            except KeyError:
                log.error(
                    "yanpd:yang_validator: Submodule {} not available.".format(subm)
                )
                return 1
            if srev is None or srev == srec["revision"]:
                sen["revision"] = srec["revision"]
            else:
                log.error(
                    "yanpd:yang_validator: Submodule {} revision mismatch.".format(subm)
                )
                return 1
            imp_only = imp_only or srec["import-only"]
            fts += srec["features"]
            sarr.append(sen)
        if fts:
            men["feature"] = fts
        if sarr:
            men["submodule"] = sarr
        men["conformance-type"] = "import" if imp_only else "implement"
        marr.append(men)
    res = {"ietf-yang-library:modules-state": {"module-set-id": "", "module": marr}}
    return json.dumps(res, indent=2)


class yang_validator:
    def __init__(self, models_dir):
        self.models_dir = models_dir
        self.models_dict = {}

        # load models
        self.load()

    def load(self):
        """
        Creates JSON-encoded YANG library data [RFC7895] and instantiates data model object out of it.
        """
        if not HAS_LIBS:
            raise RuntimeError(
                "yanpd:yang_validator: Failed to import yangson library, make sure it is installed."
            )

        # create models one per-directory under models_dir path
        for directory in os.listdir(self.models_dir):
            if directory in self.models_dict:
                log.debug(
                    "yanpd:yang_validator: model '{}' already loaded, skipping".format(
                        directory
                    )
                )
                continue
            path = os.path.join(self.models_dir, directory)
            yang_modules_library = _make_library(path)
            if log.isEnabledFor(logging.DEBUG):
                log.debug(
                    "yanpd:yang_validator: constructed '{}' YANG modules library:\n{}".format(
                        directory, yang_modules_library
                    )
                )
            self.models_dict[directory] = DataModel(
                yltxt=yang_modules_library, mod_path=[path]
            )
            if log.isEnabledFor(logging.DEBUG):
                log.debug(
                    "yanpd:yang_validator: loaded '{}' YANG model:\n{}".format(
                        directory, self.models_dict[directory].ascii_tree()
                    )
                )

    def validate(self, data, model_name, validation_scope="all", content_type="all"):
        """
        Validate data for compliance with YANG modules.

        :param data: (dict) dictionary data to validate
        :param model_name: (str) name of the model
        :param content_type: (str) optional, content type as per
            https://yangson.labs.nic.cz/enumerations.html supported - all, config, nonconfig
        :param validation_scope: (str) optional, validation scope as per
            https://yangson.labs.nic.cz/enumerations.html supported - all, semantics, syntax
		:return: (bool) True if validation succeeded False otherwise
        """
        # decide on validation scopes and content
        if validation_scope == "all":
            scope = enumerations.ValidationScope.all
        elif validation_scope == "semantics":
            scope = enumerations.ValidationScope.semantics
        elif validation_scope == "syntax":
            scope = enumerations.ValidationScope.syntax
        if content_type == "all":
            ctype = enumerations.ContentType.all
        elif content_type == "config":
            ctype = enumerations.ContentType.config
        elif content_type == "nonconfig":
            ctype = enumerations.ContentType.nonconfig

        # run validation of data
        try:
            model_content = self.models_dict[model_name]
            _data = {
                "{}:{}".format(model_name, k)
                if not k.startswith("{}:".format(model_name))
                else k: d
                for k, d in data.items()
            }
            inst = model_content.from_raw(_data)
            _ = inst.validate(scope=scope, ctype=ctype)
        except Exception as e:
            log.exception(
                "yanpd:yang_validator: validation failed.\nOriginal Data:\n'{}'\nPrepared Data:\n'{}'\nModel tree:\n'{}'".format(
                    data, _data, model_content.ascii_tree()
                )
            )
            return False

        return True
