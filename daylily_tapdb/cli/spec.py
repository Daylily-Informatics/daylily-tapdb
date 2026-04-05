from cli_core_yo.spec import CliSpec, ConfigSpec, PluginSpec, XdgSpec

spec = CliSpec(
    prog_name="tapdb",
    app_display_name="TapDB CLI",
    dist_name="daylily-tapdb",
    root_help="TapDB management commands",
    xdg=XdgSpec(app_dir_name="tapdb"),
    config=ConfigSpec(
        xdg_relative_path="config.yaml",
        template_bytes=b"environments: {}\n",
    ),
    plugins=PluginSpec(
        explicit=[
            "daylily_tapdb.cli.register",
        ]
    ),
)
