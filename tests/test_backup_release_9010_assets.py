"""TapDB dependency pinned by the inspected Bloom source baseline.

These are original 9.0.10 schema hashes. The bytes equal the retained 9.0.9
assets; this is source dependency evidence, not live deployment identification.
"""

RELEASE_COMMIT = "12351b832276d35228527c3c9596e5d60e7c7824"
BLOOM_SOURCE_COMMIT = "948bc487415899eb50f024f2d421066a061f0a48"
SOURCE_ASSET_SHA256 = {
    "tapdb_schema.sql": "64362e2882a7b424f6bf3c6b3d9c21d1e859ec98d4ee61ca2a728cecfd134584",
    "rls.sql": "b3f69e1e294340ed37795ddbbf6461c1a5ffcbd9ad5dcd4c06ad1dcd8648cefc",
    "migrations/20260303_120000_add_tenant_id.sql": "f9ff222e8732f66ce9e9e387e93ac852b6246d77ce3c08738e32b7cf010161cc",
    "migrations/20260303_120010_add_outbox_event.sql": "a8b9729c22278e799fd9e55102a665117b2c177044145738680bdd2ddd600fe9",
    "migrations/20260405_120000_add_domain_scoping.sql": "8e1dbae1a3cfc96d16cb1e56ec0b5edba99f5bce171cfbf83847ed17eeabeaa9",
    "migrations/20260612_154200_add_template_validator_ref.sql": "558e0cd14b84939551b229fe7924550a27234c8f58f9cdb781f658d1eb2735fa",
    "migrations/20260612_154210_instance_prefix_from_template.sql": "49950a3942c6a5e8eb3098d502032bf2ab55c925fe374fc522d07cd1ca3388ab",
}
