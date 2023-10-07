from dataclasses import dataclass
from typing import Type

from pulumi import Input, Output, ResourceOptions

from cvmdatalake import TableSpec


def enable_debug(flag=False):
    if flag:
        import pydevd_pycharm
        pydevd_pycharm.settrace('localhost', port=62100, stdoutToServer=True, stderrToServer=True, suspend=False)


@dataclass
class ZoneDescriptor:
    crawlers: dict[str, Input[str]]
    tables: dict[str, Input[str]]

    def merge(self, other):
        return ZoneDescriptor(
            crawlers=self.crawlers | other.crawlers,
            tables=self.tables | other.tables,
        )

    def to_pulumi_compatible_representation(self) -> dict:
        return self.__dict__


def transform_descriptor(lake_descriptor: dict) -> ZoneDescriptor:
    lake_descriptor = dict(sorted(lake_descriptor.items()))
    return ZoneDescriptor(**lake_descriptor)


def get_crawler_name(descriptor: Output[ZoneDescriptor], table: Type[TableSpec]) -> Output[str | None]:
    return descriptor.apply(lambda d: d.crawlers.get(table.table_id(), None))


def get_table_path(descriptor: Input[ZoneDescriptor], table: Type[TableSpec]) -> Output[str | None]:
    return descriptor.apply(lambda d: d.tables.get(table.table_id(), None))


def merge_descriptors(
        descriptor_a: ZoneDescriptor, descriptor_b: ZoneDescriptor, *descriptors: ZoneDescriptor
) -> ZoneDescriptor:
    result = descriptor_a.merge(descriptor_b)

    for d in descriptors:
        result = result.merge(d)

    return result


def migrate_to_terraform(environment: str) -> bool:
    return environment not in ['uat', 'prod']


def migrate_to_terraform_opts(environment: str) -> ResourceOptions:
    # this won't delete any resources. It will just remove them from pulumi state
    return ResourceOptions(retain_on_delete=environment not in ['uat', 'prod'])
