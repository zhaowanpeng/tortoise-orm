# -*- coding:utf-8 -*-
"""
@Time        :2024/7/7 上午2:00
@Author      :zhaowanpeng
@description : Pydantic model creator for Tortoise ORM models
"""
import datetime
from typing import Dict, Any, Type, List, Optional, Tuple, Union
from pydantic import BaseModel, create_model, Field
from tortoise.fields import Field as TortoiseField
from tortoise.fields.relational import RelationalField, ManyToManyFieldInstance, BackwardFKRelation
from tortoise.fields.data import JSONField, IntField, FloatField, BooleanField, CharField, TextField, DatetimeField
from tortoise.models import Model


class PydanticModelCreator:
    def __init__(
            self,
            cls: Type[Model],
            name: Optional[str] = None,
            exclude: Tuple[str, ...] = (),
            include: Tuple[str, ...] = (),
            computed: Tuple[str, ...] = (),
            optional: Tuple[str, ...] = (),
            required: Tuple[str, ...] = (),
            allow_cycles: bool = False,
            sort_alphabetically: bool = False,
    ):
        self.cls = cls
        self.name = name or f"Pydantic{cls.__name__}"
        self.exclude = set(exclude)
        self.include = set(include)
        self.computed = set(computed)
        self.optional = set(optional)
        self.required = set(required)
        self.allow_cycles = allow_cycles
        self.sort_alphabetically = sort_alphabetically

        self.field_map = self._get_field_map()

    def _get_field_map(self) -> Dict[str, dict]:
        field_map: Dict[str, dict] = {}

        # Process all model fields
        for field_name, field in self.cls._meta.fields_map.items():
            field_info = self._process_tortoise_field(field_name, field)
            if field_info:
                field_map[field_name] = field_info

        # Process backward relations
        for field_name, relation in self.cls._meta.backward_fk_fields.items():
            field_info = self._process_backward_relation(field_name, relation)
            if field_info:
                field_map[field_name] = field_info

        # Process many-to-many fields
        for field_name, m2m in self.cls._meta.m2m_fields.items():
            field_info = self._process_m2m_field(field_name, m2m)
            if field_info:
                field_map[field_name] = field_info

        # Process computed properties
        for computed_field in self.computed:
            if hasattr(self.cls, computed_field):
                field_info = self._process_computed_field(computed_field)
                if field_info:
                    field_map[computed_field] = field_info

        if self.sort_alphabetically:
            return dict(sorted(field_map.items()))
        return field_map

    def _process_tortoise_field(self, field_name: str, field: TortoiseField) -> Dict[str, Any]:
        field_info = {
            "name": field_name,
            "field_type": type(field),
            "python_type": self._get_python_type(field),
            "description": field.description,
            "required": not field.null and field_name not in self.optional,
            "default": field.default,
        }

        if isinstance(field, RelationalField):
            field_info["related_model"] = field.related_model

        if isinstance(field, JSONField):
            field_info["python_type"] = Any

        return field_info

    def _process_backward_relation(self, field_name: str, relation: BackwardFKRelation) -> Dict[str, Any]:
        return {
            "name": field_name,
            "field_type": type(relation),
            "python_type": List[relation.related_model],
            "description": f"Backward relation to {relation.related_model.__name__}",
            "required": False,
        }

    def _process_m2m_field(self, field_name: str, m2m: ManyToManyFieldInstance) -> Dict[str, Any]:
        return {
            "name": field_name,
            "field_type": type(m2m),
            "python_type": List[m2m.related_model],
            "description": f"Many-to-many relation with {m2m.related_model.__name__}",
            "required": False,
        }

    def _process_computed_field(self, field_name: str) -> Dict[str, Any]:
        computed_field = getattr(self.cls, field_name)
        return {
            "name": field_name,
            "field_type": callable,
            "python_type": Any,  # TODO: Try to infer return type from function signature
            "description": computed_field.__doc__ or "Computed field",
            "required": False,
        }

    def _get_python_type(self, field: TortoiseField) -> Type:
        type_mapping = {
            IntField: int,
            FloatField: float,
            BooleanField: bool,
            CharField: str,
            TextField: str,
            DatetimeField: datetime.datetime,
            JSONField: Any,
            RelationalField: lambda f: f.related_model,
        }

        for field_type, python_type in type_mapping.items():
            if isinstance(field, field_type):
                return python_type(field) if callable(python_type) else python_type

        return Any  # Default to Any for unknown types

    def _create_pydantic_model(self) -> Type[BaseModel]:
        properties = {}
        for field_name, field_info in self.field_map.items():
            if self._should_include_field(field_name):
                field_type, field_config = self._process_field(field_name, field_info)
                properties[field_name] = (field_type, field_config)

        model = create_model(
            self.name,
            __base__=BaseModel,
            **properties
        )
        model.__doc__ = self.cls.__doc__
        return model

    def _should_include_field(self, field_name: str) -> bool:
        if field_name in self.exclude:
            return False
        if self.include and field_name not in self.include:
            return False
        return True

    def _process_field(self, field_name: str, field_info: Dict[str, Any]) -> Tuple[Type, Field]:
        field_type = field_info['python_type']
        field_config = {
            'description': field_info.get('description'),
            'default': field_info.get('default'),
        }

        if field_name in self.required:
            field_config['required'] = True
        elif field_name in self.optional or not field_info.get('required', True):
            field_type = Optional[field_type]

        return field_type, Field(**field_config)

    def create_model(self) -> Type[BaseModel]:
        return self._create_pydantic_model()


def pydantic_model_creator(
        cls: Type[Model],
        *,
        name: Optional[str] = None,
        exclude: Tuple[str, ...] = (),
        include: Tuple[str, ...] = (),
        computed: Tuple[str, ...] = (),
        optional: Tuple[str, ...] = (),
        required: Tuple[str, ...] = (),
        allow_cycles: bool = False,
        sort_alphabetically: bool = False,
) -> Type[BaseModel]:
    creator = PydanticModelCreator(
        cls,
        name=name,
        exclude=exclude,
        include=include,
        computed=computed,
        optional=optional,
        required=required,
        allow_cycles=allow_cycles,
        sort_alphabetically=sort_alphabetically,
    )
    return creator.create_model()