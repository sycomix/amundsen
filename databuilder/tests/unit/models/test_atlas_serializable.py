# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0
import unittest
from typing import (
    Iterable, Iterator, Union,
)

from amundsen_common.utils.atlas import AtlasCommonParams

from databuilder.models.atlas_entity import AtlasEntity
from databuilder.models.atlas_relationship import AtlasRelationship
from databuilder.models.atlas_serializable import AtlasSerializable
from databuilder.serializers import atlas_serializer
from databuilder.utils.atlas import AtlasSerializedEntityFields, AtlasSerializedEntityOperation


class TestSerialize(unittest.TestCase):

    def test_serialize(self) -> None:
        actors = [Actor('Tom Cruise'), Actor('Meg Ryan')]
        cities = [City('San Diego'), City('Oakland')]
        movie = Movie('Top Gun', actors, cities)

        actual = []
        while entity := movie.next_atlas_entity():
            actual.append(atlas_serializer.serialize_entity(entity))
        expected = [
            {
                'name': 'Tom Cruise',
                'operation': 'CREATE',
                'qualifiedName': 'actor://Tom Cruise',
                'relationships': None,
                'typeName': 'Actor',
            },
            {
                'name': 'Meg Ryan',
                'operation': 'CREATE',
                'qualifiedName': 'actor://Meg Ryan',
                'relationships': None,
                'typeName': 'Actor',
            },
            {
                'name': 'San Diego',
                'operation': 'CREATE',
                'qualifiedName': 'city://San Diego',
                'relationships': None,
                'typeName': 'City',
            },
            {
                'name': 'Oakland',
                'operation': 'CREATE',
                'qualifiedName': 'city://Oakland',
                'relationships': None,
                'typeName': 'City',
            },
            {
                'name': 'Top Gun',
                'operation': 'CREATE',
                'qualifiedName': 'movie://Top Gun',
                'relationships': 'actors#ACTOR#actor://Tom Cruise|actors#ACTOR#actor://Meg Ryan',
                'typeName': 'Movie',
            },
        ]

        self.assertEqual(expected, actual)

        actual = []
        while relation := movie.next_atlas_relation():
            actual.append(atlas_serializer.serialize_relationship(relation))
        expected = [
            {
                'entityQualifiedName1': 'movie://Top Gun',
                'entityQualifiedName2': 'city://San Diego',
                'entityType1': 'Movie',
                'entityType2': 'City',
                'relationshipType': 'FILMED_AT',
            },
            {
                'entityQualifiedName1': 'movie://Top Gun',
                'entityQualifiedName2': 'city://Oakland',
                'entityType1': 'Movie',
                'entityType2': 'City',
                'relationshipType': 'FILMED_AT',
            },
        ]
        self.assertEqual(expected, actual)


class Actor:
    TYPE = 'Actor'
    KEY_FORMAT = 'actor://{}'

    def __init__(self, name: str) -> None:
        self.name = name


class City:
    TYPE = 'City'
    KEY_FORMAT = 'city://{}'

    def __init__(self, name: str) -> None:
        self.name = name


class Movie(AtlasSerializable):
    TYPE = 'Movie'
    KEY_FORMAT = 'movie://{}'
    MOVIE_ACTOR_RELATION_TYPE = 'ACTOR'
    MOVIE_CITY_RELATION_TYPE = 'FILMED_AT'

    def __init__(
        self,
        name: str,
        actors: Iterable[Actor],
        cities: Iterable[City],
    ) -> None:
        self._name = name
        self._actors = actors
        self._cities = cities
        self._entity_iter = iter(self._create_next_atlas_entity())
        self._relation_iter = iter(self._create_next_atlas_relation())

    def create_next_atlas_entity(self) -> Union[AtlasEntity, None]:
        try:
            return next(self._entity_iter)
        except StopIteration:
            return None

    def create_next_atlas_relation(self) -> Union[AtlasRelationship, None]:
        try:
            return next(self._relation_iter)
        except StopIteration:
            return None

    def _create_next_atlas_entity(self) -> Iterable[AtlasEntity]:

        for actor in self._actors:
            attrs_mapping = [
                (AtlasCommonParams.qualified_name, actor.KEY_FORMAT.format(actor.name)),
                ('name', actor.name),
            ]

            actor_entity_attrs = {}
            for attr in attrs_mapping:
                attr_key, attr_value = attr
                actor_entity_attrs[attr_key] = attr_value

            yield AtlasEntity(
                typeName=actor.TYPE,
                operation=AtlasSerializedEntityOperation.CREATE,
                attributes=actor_entity_attrs,
                relationships=None,
            )
        for city in self._cities:
            attrs_mapping = [
                (AtlasCommonParams.qualified_name, city.KEY_FORMAT.format(city.name)),
                ('name', city.name),
            ]

            city_entity_attrs = {}
            for attr in attrs_mapping:
                attr_key, attr_value = attr
                city_entity_attrs[attr_key] = attr_value

            yield AtlasEntity(
                typeName=city.TYPE,
                operation=AtlasSerializedEntityOperation.CREATE,
                attributes=city_entity_attrs,
                relationships=None,
            )
        attrs_mapping = [
            (AtlasCommonParams.qualified_name, self.KEY_FORMAT.format(self._name)),
            ('name', self._name),
        ]

        movie_entity_attrs = {}
        for attr in attrs_mapping:
            attr_key, attr_value = attr
            movie_entity_attrs[attr_key] = attr_value

        """
        relationship in form 'relation_attribute#relation_entity_type#qualified_name_of_related_object
        """
        relationship_list = [
            AtlasSerializedEntityFields.relationships_kv_separator.join(
                (
                    'actors',
                    self.MOVIE_ACTOR_RELATION_TYPE,
                    actor.KEY_FORMAT.format(actor.name),
                )
            )
            for actor in self._actors
        ]
        yield AtlasEntity(
            typeName=self.TYPE,
            operation=AtlasSerializedEntityOperation.CREATE,
            attributes=movie_entity_attrs,
            relationships=AtlasSerializedEntityFields.relationships_separator.join(
                relationship_list
            ),
        )

    def _create_next_atlas_relation(self) -> Iterator[AtlasRelationship]:
        for city in self._cities:
            yield AtlasRelationship(
                relationshipType=self.MOVIE_CITY_RELATION_TYPE,
                entityType1=self.TYPE,
                entityQualifiedName1=self.KEY_FORMAT.format(self._name),
                entityType2=city.TYPE,
                entityQualifiedName2=city.KEY_FORMAT.format(city.name),
                attributes={},
            )


if __name__ == '__main__':
    unittest.main()
