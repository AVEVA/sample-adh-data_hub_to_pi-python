
from adh_sample_library_preview import (
    SdsResolvedStream, SdsType, SdsExtrapolationMode, SdsInterpolationMode, SdsStreamIndex, SdsStreamPropertyOverride)


class CustomResolvedStream(SdsResolvedStream, object):
    """Custom resolved stream definition"""

    def __init__(self, id: str = None, type_id: str = None, name: str = None, tenant_id: str = None, tenant_name: str = None,
                 namespace_id: str = None,
                 community_id: str = None,
                 description: str = None,
                 indexes: list[SdsStreamIndex] = None,
                 index_id: str = None,
                 interpolation_mode: SdsInterpolationMode = None,
                 extrapolation_mode: SdsExtrapolationMode = None,
                 property_overrides: list[SdsStreamPropertyOverride] = None,
                 resolved=True, type: SdsType = None):
        """
        :param id: required
        :param type_id: not required
        :param name: not required
        :param tenant_id: not required
        :param tenant_name: not required
        :param namespace_id: not required
        :param community_id: not required
        :param description: not required
        :param indexes: array of SdsStreamIndex   not required
        :param index_id: not required
        :param interpolation_mode: SdsInterpolationMode   default is null
                                   not required
        :param extrapolation_mode: SdsExtrapolationMode default is null
                                  not required
        :param property_overrides:  array of  SdsStreamPropertyOverride
                                   not required
        :param resolved: not required
        :param type: required
        """
        self.Id = id
        self.TypeId = type_id
        self.Name = name
        self.TenantId = tenant_id
        self.TenantName = tenant_name
        self.NamespaceId = namespace_id
        self.CommunityId = community_id
        self.Description = description
        self.Indexes = indexes
        self.IndexId = index_id
        self.InterpolationMode = interpolation_mode
        self.ExtrapolationMode = extrapolation_mode
        self.PropertyOverrides = property_overrides
        self.Resolved = resolved
        self.Type = type

    @property
    def TenantId(self) -> str:
        return self.__tenant_id

    @TenantId.setter
    def TenantId(self, value: str):
        self.__tenant_id = value

    @property
    def TenantName(self) -> str:
        return self.__tenant_name

    @TenantName.setter
    def TenantName(self, value: str):
        self.__tenant_name = value

    @property
    def NamespaceId(self) -> str:
        return self.__namespace_id

    @NamespaceId.setter
    def NamespaceId(self, value: str):
        self.__namespace_id = value

    @property
    def CommunityId(self) -> str:
        return self.__community_id

    @CommunityId.setter
    def CommunityId(self, value: str):
        self.__community_id = value

    @property
    def IndexId(self) -> str:
        return self.__index_id

    @IndexId.setter
    def IndexId(self, value: str):
        self.__index_id = value

    def toDictionary(self):
        result = super().toDictionary()

        if self.TenantId is not None:
            result['TenantId'] = self.TenantId

        if self.TenantName is not None:
            result['TenantName'] = self.TenantName

        if self.NamespaceId is not None:
            result['NamespaceId'] = self.NamespaceId

        if self.CommunityId is not None:
            result['CommunityId'] = self.CommunityId

        if self.IndexId is not None:
            result['IndexId'] = self.IndexId

        return result

    @staticmethod
    def fromJson(content: dict[str, str]):
        result = CustomResolvedStream()

        if not content:
            return result

        if 'Id' in content:
            result.Id = content['Id']

        if 'TenantId' in content:
            result.TenantId = content['TenantId']

        if 'TenantName' in content:
            result.TenantName = content['TenantName']

        if 'NamespaceId' in content:
            result.NamespaceId = content['NamespaceId']

        if 'CommunityId' in content:
            result.CommunityId = content['CommunityId']

        if 'Name' in content:
            result.Name = content['Name']

        if 'Description' in content:
            result.Description = content['Description']

        if 'TypeId' in content:
            result.TypeId = content['TypeId']

        if 'Indexes' in content:
            indexes = content['Indexes']
            if indexes is not None and len(indexes) > 0:
                result.Indexes = []
                for value in indexes:
                    result.Indexes.append(SdsStreamIndex.fromJson(value))

        if 'IndexId' in content:
            result.IndexId = content['IndexId']

        if 'InterpolationMode' in content:
            interpolation_mode = content['InterpolationMode']
            if interpolation_mode is not None:
                result.InterpolationMode = SdsInterpolationMode[interpolation_mode]

        if 'ExtrapolationMode' in content:
            extrapolation_mode = content['ExtrapolationMode']
            if extrapolation_mode is not None:
                result.ExtrapolationMode = SdsExtrapolationMode[extrapolation_mode]

        if 'PropertyOverrides' in content:
            property_overrides = content['PropertyOverrides']
            if property_overrides is not None and len(property_overrides) > 0:
                result.PropertyOverrides = []
                for value in property_overrides:
                    result.PropertyOverrides.append(
                        SdsStreamPropertyOverride.fromJson(value))

        if 'Resolved' in content:
            result.Resolved = content['Resolved']

        if 'Type' in content:
            result.Type = SdsType.fromJson(content['Type'])

        return result

    @staticmethod
    def fromResolvedStream(resolved_stream: SdsResolvedStream):
        return CustomResolvedStream.fromJson(resolved_stream.toDictionary())
