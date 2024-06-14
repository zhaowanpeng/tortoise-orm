# -*- coding:utf-8 -*-
"""
@Time        :2024/6/14 下午6:32
@Author      :zhaowanpeng
@description :
"""
from tortoise import Tortoise
from tortoise.models import Model
from tortoise.exceptions import ConfigurationError
from tortoise.connection import connections
from tortoise.filters import get_m2m_filters
from tortoise.backends.base.config_generator import generate_config
from tortoise.fields.relational import (
    BackwardFKRelation,
    BackwardOneToOneRelation,
    ForeignKeyFieldInstance,
    ManyToManyFieldInstance,
    OneToOneFieldInstance,
)
from tortoise.utils import generate_schema_for_client
from typing import Iterable, Union, List, Type, Tuple, cast, Dict, Set
from types import ModuleType
from copy import deepcopy
from pypika import Table
# from loguru import logger


class TameTortoise(Tortoise):
    dburl_name: Dict = {}
    apps_modules: Dict = {}

    @classmethod
    def _init_relations_alone(cls, alone_app_name: str) -> None:

        def get_related_model(related_app_name_: str, related_model_name_: str) -> Type["Model"]:
            try:
                return cls.apps[related_app_name_][related_model_name_]
            except KeyError:
                if related_app_name_ not in cls.apps:
                    raise ConfigurationError(
                        f"No app with name '{related_app_name_}' registered."
                        f" Please check your model names in ForeignKeyFields"
                        f" and configurations."
                    )
                raise ConfigurationError(
                    f"No model with name '{related_model_name_}' registered in"
                    f" app '{related_app_name_}'."
                )

        def split_reference(reference_: str) -> Tuple[str, str]:
            """
            Test, if reference follow the official naming conventions. Throws a
            ConfigurationError with a hopefully helpful message. If successful,
            returns the app and the model name.

            :raises ConfigurationError: If no model reference is invalid.
            """
            items = reference_.split(".")
            if len(items) != 2:  # pragma: nocoverage
                raise ConfigurationError(
                    (
                        "'%s' is not a valid model reference Bad Reference."
                        " Should be something like <appname>.<modelname>."
                    )
                    % reference
                )

            return (items[0], items[1])

        # for app_name, app in {alone_app_name: cls.apps[alone_app_name]}:
        app_name = alone_app_name

        for model_name, model in cls.apps[alone_app_name].items():
            if model._meta._inited:
                continue
            model._meta._inited = True
            if not model._meta.db_table:
                model._meta.db_table = model.__name__.lower()

            for field in sorted(model._meta.fk_fields):
                fk_object = cast(ForeignKeyFieldInstance, model._meta.fields_map[field])
                reference = fk_object.model_name
                related_app_name, related_model_name = split_reference(reference)
                related_model = get_related_model(related_app_name, related_model_name)

                if fk_object.to_field:
                    related_field = related_model._meta.fields_map.get(fk_object.to_field, None)
                    if related_field:
                        if related_field.unique:
                            key_fk_object = deepcopy(related_field)
                            fk_object.to_field_instance = related_field  # type: ignore
                        else:
                            raise ConfigurationError(
                                f'field "{fk_object.to_field}" in model'
                                f' "{related_model_name}" is not unique'
                            )
                    else:
                        raise ConfigurationError(
                            f'there is no field named "{fk_object.to_field}"'
                            f' in model "{related_model_name}"'
                        )
                else:
                    key_fk_object = deepcopy(related_model._meta.pk)
                    fk_object.to_field_instance = related_model._meta.pk  # type: ignore
                    fk_object.to_field = related_model._meta.pk_attr
                fk_object.field_type = fk_object.to_field_instance.field_type
                key_field = f"{field}_id"
                key_fk_object.pk = False
                key_fk_object.unique = False
                key_fk_object.index = fk_object.index
                key_fk_object.default = fk_object.default
                key_fk_object.null = fk_object.null
                key_fk_object.generated = fk_object.generated
                key_fk_object.reference = fk_object
                key_fk_object.description = fk_object.description
                if fk_object.source_field:
                    key_fk_object.source_field = fk_object.source_field
                else:
                    key_fk_object.source_field = key_field
                model._meta.add_field(key_field, key_fk_object)

                fk_object.related_model = related_model
                fk_object.source_field = key_field
                backward_relation_name = fk_object.related_name
                if backward_relation_name is not False:
                    if not backward_relation_name:
                        backward_relation_name = f"{model._meta.db_table}s"
                    if backward_relation_name in related_model._meta.fields:
                        raise ConfigurationError(
                            f'backward relation "{backward_relation_name}" duplicates in'
                            f" model {related_model_name}"
                        )
                    fk_relation = BackwardFKRelation(
                        model,
                        f"{field}_id",
                        key_fk_object.source_field,
                        fk_object.null,
                        fk_object.description,
                    )
                    fk_relation.to_field_instance = fk_object.to_field_instance  # type: ignore
                    related_model._meta.add_field(backward_relation_name, fk_relation)

            for field in model._meta.o2o_fields:
                o2o_object = cast(OneToOneFieldInstance, model._meta.fields_map[field])
                reference = o2o_object.model_name
                related_app_name, related_model_name = split_reference(reference)
                related_model = get_related_model(related_app_name, related_model_name)

                if o2o_object.to_field:
                    related_field = related_model._meta.fields_map.get(
                        o2o_object.to_field, None
                    )
                    if related_field:
                        if related_field.unique:
                            key_o2o_object = deepcopy(related_field)
                            o2o_object.to_field_instance = related_field  # type: ignore
                        else:
                            raise ConfigurationError(
                                f'field "{o2o_object.to_field}" in model'
                                f' "{related_model_name}" is not unique'
                            )
                    else:
                        raise ConfigurationError(
                            f'there is no field named "{o2o_object.to_field}"'
                            f' in model "{related_model_name}"'
                        )
                else:
                    key_o2o_object = deepcopy(related_model._meta.pk)
                    o2o_object.to_field_instance = related_model._meta.pk  # type: ignore
                    o2o_object.to_field = related_model._meta.pk_attr

                o2o_object.field_type = o2o_object.to_field_instance.field_type

                key_field = f"{field}_id"
                key_o2o_object.pk = o2o_object.pk
                key_o2o_object.index = o2o_object.index
                key_o2o_object.default = o2o_object.default
                key_o2o_object.null = o2o_object.null
                key_o2o_object.unique = o2o_object.unique
                key_o2o_object.generated = o2o_object.generated
                key_o2o_object.reference = o2o_object
                key_o2o_object.description = o2o_object.description
                if o2o_object.source_field:
                    key_o2o_object.source_field = o2o_object.source_field
                else:
                    key_o2o_object.source_field = key_field
                model._meta.add_field(key_field, key_o2o_object)

                o2o_object.related_model = related_model
                o2o_object.source_field = key_field
                backward_relation_name = o2o_object.related_name
                if backward_relation_name is not False:
                    if not backward_relation_name:
                        backward_relation_name = f"{model._meta.db_table}"
                    if backward_relation_name in related_model._meta.fields:
                        raise ConfigurationError(
                            f'backward relation "{backward_relation_name}" duplicates in'
                            f" model {related_model_name}"
                        )
                    o2o_relation = BackwardOneToOneRelation(
                        model,
                        f"{field}_id",
                        key_o2o_object.source_field,
                        null=True,
                        description=o2o_object.description,
                    )
                    o2o_relation.to_field_instance = o2o_object.to_field_instance  # type: ignore
                    related_model._meta.add_field(backward_relation_name, o2o_relation)

                if o2o_object.pk:
                    model._meta.pk_attr = key_field

            for field in list(model._meta.m2m_fields):
                m2m_object = cast(ManyToManyFieldInstance, model._meta.fields_map[field])
                if m2m_object._generated:
                    continue

                backward_key = m2m_object.backward_key
                if not backward_key:
                    backward_key = f"{model._meta.db_table}_id"
                    if backward_key == m2m_object.forward_key:
                        backward_key = f"{model._meta.db_table}_rel_id"
                    m2m_object.backward_key = backward_key

                reference = m2m_object.model_name
                related_app_name, related_model_name = split_reference(reference)
                related_model = get_related_model(related_app_name, related_model_name)

                m2m_object.related_model = related_model

                backward_relation_name = m2m_object.related_name
                if not backward_relation_name:
                    backward_relation_name = (
                        m2m_object.related_name
                    ) = f"{model._meta.db_table}s"
                if backward_relation_name in related_model._meta.fields:
                    raise ConfigurationError(
                        f'backward relation "{backward_relation_name}" duplicates in'
                        f" model {related_model_name}"
                    )

                if not m2m_object.through:
                    related_model_table_name = (
                        related_model._meta.db_table
                        if related_model._meta.db_table
                        else related_model.__name__.lower()
                    )

                    m2m_object.through = f"{model._meta.db_table}_{related_model_table_name}"

                m2m_relation = ManyToManyFieldInstance(
                    f"{app_name}.{model_name}",
                    m2m_object.through,
                    forward_key=m2m_object.backward_key,
                    backward_key=m2m_object.forward_key,
                    related_name=field,
                    field_type=model,
                    description=m2m_object.description,
                )
                m2m_relation._generated = True
                model._meta.filters.update(get_m2m_filters(field, m2m_object))
                related_model._meta.add_field(backward_relation_name, m2m_relation)

    @classmethod
    async def load_app(cls,
                       app_name: str,
                       models_paths: Iterable[Union[ModuleType, str]],
                       db_url: str,
                       use_tz: bool = False,
                       timezone: str = "Asia/Shanghai",
                       generate: bool = False,
                       create_db: bool = False,
                       ) -> None:
        # 如果是字符串,处理为列表
        models_paths = [models_paths] if isinstance(models_paths, str) else models_paths

        if app_name in cls.apps_modules:
            raise ValueError(f"{app_name} has already been initialized.")
        cls.apps_modules[app_name] = models_paths

        # db_url's name default use 'default'
        cls.dburl_name[db_url] = "default"
        # if db_url is new, use app_name as its name
        if db_url not in cls.dburl_name:
            cls.dburl_name[db_url] = app_name

        # create config
        db_name = cls.dburl_name[db_url]
        config = generate_config(db_url, {app_name: models_paths}, db_name)

        # init timezone
        cls._init_timezone(use_tz, timezone)

        # init db
        connections_config = config["connections"]
        await connections._init(connections_config, create_db)

        # init app models
        apps_config = config["apps"]
        cls._init_apps_alone(apps_config, app_name)

        cls._init_routers(None)
        cls._inited = True

    @classmethod
    def _init_apps_alone(cls, apps_config: dict, app_name: str) -> None:
        for name, info in apps_config.items():
            try:
                connections.get(info.get("default_connection", "default"))
            except KeyError:
                raise ConfigurationError(
                    'Unknown connection "{}" for app "{}"'.format(
                        info.get("default_connection", "default"), name
                    )
                )

            cls.init_models(info["models"], name, _init_relations=False)

            for model in cls.apps[name].values():
                model._meta.default_connection = info.get("default_connection", "default")

        cls._init_relations_alone(app_name)

        cls._build_initial_querysets_alone(app_name)

    @classmethod
    def _build_initial_querysets_alone(cls, app_name: str) -> None:

        for model in cls.apps[app_name].values():
            model._meta.finalise_model()
            model._meta.basetable = Table(name=model._meta.db_table, schema=model._meta.schema)
            model._meta.basequery = model._meta.db.query_class.from_(model._meta.basetable)
            model._meta.basequery_all_fields = model._meta.basequery.select(
                *model._meta.db_fields
            )

    @classmethod
    async def generate_app_schemas(cls, app_name: str, safe: bool = True) -> None:

        if app_name not in cls.apps_modules:
            raise ConfigurationError(f"You have to call app_load({app_name},...) first before generating schemas")

        # connection = connections.get()
        for connection in connections.all():
            await generate_schema_for_client(connection, safe)