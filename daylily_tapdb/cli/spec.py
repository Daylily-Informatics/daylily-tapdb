from cli_core_yo.spec import (
    CliSpec,
    ConfigSpec,
    ContextOptionSpec,
    InvocationContextSpec,
    PluginSpec,
    PolicySpec,
    XdgSpec,
)

spec = CliSpec(
    prog_name="tapdb",
    app_display_name="TapDB CLI",
    dist_name="daylily-tapdb",
    root_help="TapDB management commands",
    xdg=XdgSpec(app_dir_name="tapdb"),
    policy=PolicySpec(),
    config=ConfigSpec(
        xdg_relative_path="config.yaml",
        template_bytes=b"environments: {}\n",
    ),
    context=InvocationContextSpec(
        options=[
            ContextOptionSpec(
                name="env_name",
                option_flags=("--env",),
                value_type="str",
                help="Explicit TapDB environment name for this invocation.",
            ),
            ContextOptionSpec(
                name="client_id",
                option_flags=("--client-id",),
                value_type="str",
                help="Namespace metadata key for config init and migration flows.",
            ),
            ContextOptionSpec(
                name="database_name",
                option_flags=("--database-name",),
                value_type="str",
                help="Database namespace key for config init and migration flows.",
            ),
        ]
    ),
    plugins=PluginSpec(
        explicit=[
            "daylily_tapdb.cli.register",
        ]
    ),
)
