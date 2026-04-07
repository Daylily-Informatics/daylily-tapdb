import inspect

from cli_core_yo.spec import CliSpec, ConfigSpec, PluginSpec, XdgSpec

try:
    from cli_core_yo.spec import PolicySpec
except ImportError:  # pragma: no cover - older cli-core-yo releases
    PolicySpec = None


_SPEC_KWARGS = {
    "prog_name": "tapdb",
    "app_display_name": "TapDB CLI",
    "dist_name": "daylily-tapdb",
    "root_help": "TapDB management commands",
    "xdg": XdgSpec(app_dir_name="tapdb"),
    "config": ConfigSpec(
        xdg_relative_path="config.yaml",
        template_bytes=b"environments: {}\n",
    ),
    "plugins": PluginSpec(
        explicit=[
            "daylily_tapdb.cli.register",
        ]
    ),
}

if PolicySpec is not None and "policy" in inspect.signature(CliSpec).parameters:
    _SPEC_KWARGS["policy"] = PolicySpec()

spec = CliSpec(**_SPEC_KWARGS)
