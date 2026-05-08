from md_python.models import RegisteredModule


def _module(input_settings):
    return RegisteredModule(
        id="anova_volcano_plot",
        name="ANOVA Volcano Plot",
        group="ANOVA",
        icon="md-icon-plot-volcano",
        input_settings=input_settings,
    )


class TestSettingKeys:
    def test_array_form_extracts_keys(self):
        mod = _module(
            [
                {"key": "datasetsSearch", "required": True},
                {"key": "proteinLists", "required": False},
            ]
        )
        assert mod.setting_keys() == ["datasetsSearch", "proteinLists"]
        assert mod.required_setting_keys() == ["datasetsSearch"]

    def test_dict_form_extracts_keys(self):
        # Some modules on the live registry use the mapping form
        # `{key: spec, ...}` rather than a list. The Ruby model accepts both
        # (see Workspaces::TabModule#setting_keys_for).
        mod = _module(
            {
                "datasetsSearch": {"field": "x"},
                "proteinLists": {"field": "y"},
            }
        )
        assert sorted(mod.setting_keys()) == ["datasetsSearch", "proteinLists"]
        # required_setting_keys is array-shape-only — mapping shape returns []
        assert mod.required_setting_keys() == []

    def test_no_schema(self):
        mod = _module(None)
        assert mod.setting_keys() == []
        assert mod.required_setting_keys() == []


class TestValidateSettingsKeys:
    def test_subset_passes(self):
        mod = _module([{"key": "datasetsSearch"}, {"key": "proteinLists"}])
        assert mod.validate_settings_keys({"datasetsSearch": []}) == []

    def test_unknown_key_returned_sorted(self):
        mod = _module([{"key": "datasetsSearch"}, {"key": "proteinLists"}])
        unknown = mod.validate_settings_keys(
            {"datasetsSearch": [], "wrong": 1, "alsoWrong": 2}
        )
        assert unknown == ["alsoWrong", "wrong"]

    def test_no_schema_skips_validation(self):
        # Mirrors the server: when input_settings is missing, the keys check
        # is skipped (settings_keys_must_match_registered_module returns nil).
        mod = _module(None)
        assert mod.validate_settings_keys({"anything": 1}) == []


class TestDefaults:
    def test_array_form_picks_up_non_null_defaults(self):
        # Mirrors the heading module shape from the live registry.
        mod = _module(
            [
                {"key": "text", "default": None, "required": True},
                {"key": "size", "default": "h1", "required": True},
                {"key": "horizontalPosition", "default": "left", "required": True},
                {"key": "verticalPosition", "default": "middle", "required": True},
            ]
        )
        assert mod.defaults() == {
            "size": "h1",
            "horizontalPosition": "left",
            "verticalPosition": "middle",
        }

    def test_dict_form_defaults(self):
        mod = _module(
            {
                "size": {"default": "h1"},
                "text": {"default": None},
                "noSpec": "stringy",
            }
        )
        assert mod.defaults() == {"size": "h1"}

    def test_no_input_settings(self):
        assert _module(None).defaults() == {}


class TestRequiredKeysAcrossShapes:
    """The wire format for input_settings differs between the cached manifest
    (array-shape, `required: true`) and the live `/module_registry` endpoint
    (dict-shape, `rules: [{name: "is_required"}]`)."""

    def test_dict_shape_with_rules_is_required(self):
        # Mirrors the live dev API payload for `heading`: dict shape, with
        # required-ness encoded under `rules`.
        mod = _module(
            {
                "text": {
                    "fieldType": "String",
                    "rules": [{"name": "is_required"}],
                },
                "size": {
                    "fieldType": "String",
                    "default": "h1",
                    "rules": [{"name": "is_required"}],
                },
                "optional_thing": {"fieldType": "String"},
            }
        )
        assert sorted(mod.required_setting_keys()) == ["size", "text"]

    def test_dict_shape_without_rules(self):
        # No required marker → treat as not required.
        mod = _module({"foo": {"fieldType": "String"}})
        assert mod.required_setting_keys() == []

    def test_array_shape_still_works(self):
        # Belt-and-braces: don't regress the cached-manifest array shape.
        mod = _module(
            [
                {"key": "text", "required": True},
                {"key": "size", "required": False},
            ]
        )
        assert mod.required_setting_keys() == ["text"]

    def test_array_shape_with_rules_encoding(self):
        # In case array entries ever start carrying `rules` instead of/as well
        # as `required: true`.
        mod = _module([{"key": "text", "rules": [{"name": "is_required"}]}])
        assert mod.required_setting_keys() == ["text"]


class TestMissingRequiredKeys:
    def test_user_supplies_required_no_default(self):
        mod = _module(
            [
                {"key": "text", "default": None, "required": True},
                {"key": "size", "default": "h1", "required": True},
            ]
        )
        # text is satisfied by user, size is satisfied by default
        assert mod.missing_required_keys({"text": "Hello"}) == []

    def test_user_omits_required_no_default(self):
        mod = _module(
            [
                {"key": "text", "default": None, "required": True},
                {"key": "size", "default": "h1", "required": True},
            ]
        )
        assert mod.missing_required_keys({}) == ["text"]

    def test_no_required_keys(self):
        mod = _module([{"key": "foo", "default": "x", "required": False}])
        assert mod.missing_required_keys({}) == []


class TestFromJson:
    def test_camel_case_remapped(self):
        mod = RegisteredModule.from_json(
            {
                "id": "heatmap",
                "name": "Heatmap",
                "group": "Heatmap",
                "icon": "md-icon-heatmap",
                "shortName": "Heatmap",
                "shortDescription": "...",
                "instructionName": "ModuleHeatmap",
                "keywords": ["heatmap"],
                "input_settings": [{"key": "datasetsSearch"}],
            }
        )
        assert mod.short_name == "Heatmap"
        assert mod.short_description == "..."
        assert mod.instruction_name == "ModuleHeatmap"
        assert mod.keywords == ["heatmap"]
