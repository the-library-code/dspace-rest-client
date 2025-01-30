# This software is licenced under the BSD 3-Clause licence
# available at https://opensource.org/licenses/BSD-3-Clause
# and described in the LICENSE.txt file in the root of this project

"""
DSpace REST API client library models.
Intended to make interacting with DSpace in Python 3 easier, particularly
when creating, updating, retrieving and deleting DSpace Objects.

@author Kim Shepherd <kim@shepherd.nz>
"""
import json

__all__ = ['DSpaceObject', 'HALResource', 'ExternalDataObject', 'SimpleDSpaceObject', 'Community',
           'Collection', 'Item', 'Bundle', 'Bitstream', 'BitstreamFormat', 'User', 'Group']


class HALResource:
    """
    Base class to represent HAL+JSON API resources
    """
    links = {}
    type = None
    embedded = dict()

    def __init__(self, api_resource=None):
        """
        Default constructor
        @param api_resource: optional API resource (JSON) from a GET response or successful POST can populate instance
        """
        if api_resource is not None:
            if 'type' in api_resource:
                self.type = api_resource['type']
            if '_links' in api_resource:
                self.links = api_resource['_links'].copy()
            if '_embedded' in api_resource:
                self.embedded = api_resource['_embedded'].copy()
            else:
                self.links = {'self': {'href': None}}

class AddressableHALResource(HALResource):
    id = None
    def __init__(self, api_resource=None):
        super().__init__(api_resource)
        if api_resource is not None:
            if 'id' in api_resource:
                self.id = api_resource['id']

    def as_dict(self):
        return {'id': self.id}

class ExternalDataObject(HALResource):
    """
    Generic External Data Object as configured in DSpace's external data providers framework
    """
    id = None
    display = None
    value = None
    externalSource = None
    metadata = {}

    def __init__(self, api_resource=None):
        """
        Default constructor
        @param api_resource: optional API resource (JSON) from a GET response or successful POST can populate instance
        """
        super().__init__(api_resource)

        self.metadata = {}

        if api_resource is not None:
            if 'id' in api_resource:
                self.id = api_resource['id']
            if 'display' in api_resource:
                self.display = api_resource['display']
            if 'value' in api_resource:
                self.value = api_resource['value']
            if 'externalSource' in api_resource:
                self.externalSource = api_resource['externalSource']
            if 'metadata' in api_resource:
                self.metadata = api_resource['metadata'].copy()

    def get_metadata_values(self, field):
        """
        Return metadata values as simple list of strings
        @param field: DSpace field, eg. dc.creator
        @return: list of strings
        """
        values = []
        if field in self.metadata:
            values = self.metadata[field]
        return values


class DSpaceObject(HALResource):
    """
    Base class to represent DSpaceObject API resources
    The variables here are present in an _embedded response and the ones required for POST / PUT / PATCH
    operations are included in the dict returned by asDict(). Implements toJSON() as well.
    This class can be used on its own but is generally expected to be extended by other types: Item, Bitstream, etc.
    """
    uuid = None
    name = None
    handle = None
    metadata = {}
    lastModified = None
    type = None
    parent = None

    def __init__(self, api_resource=None, dso=None):
        """
        Default constructor
        @param api_resource: optional API resource (JSON) from a GET response or successful POST can populate instance
        """
        super().__init__(api_resource)
        self.type = None
        self.metadata = {}

        if dso is not None:
            api_resource = dso.as_dict()
            self.links = dso.links.copy()
        if api_resource is not None:
            if 'id' in api_resource:
                self.id = api_resource['id']
            if 'uuid' in api_resource:
                self.uuid = api_resource['uuid']
            if 'type' in api_resource:
                self.type = api_resource['type']
            if 'name' in api_resource:
                self.name = api_resource['name']
            if 'handle' in api_resource:
                self.handle = api_resource['handle']
            if 'metadata' in api_resource:
                self.metadata = api_resource['metadata'].copy()
            # Python interprets _ prefix as private so for now, renaming this and handling it separately
            # alternatively - each item could implement getters, or a public method to return links
            if '_links' in api_resource:
                self.links = api_resource['_links'].copy()

    def add_metadata(self, field, value, language=None, authority=None, confidence=-1, place=None):
        """
        Add metadata to a DSO. This is performed on the local object only, it is not an API operation (see patch)
        This is useful when constructing new objects for ingest.
        When doing simple changes like "retrieve a DSO, add some metadata, update" then it is best to use a patch
        operation, not this clas method. See
        :param field:
        :param value:
        :param language:
        :param authority:
        :param confidence:
        :param place:
        :return:
        """
        if field is None or value is None:
            return
        if field in self.metadata:
            values = self.metadata[field]
            # Ensure we don't accidentally duplicate place value. If this place already exists, the user
            # should use a patch operation or we should allow another way to re-order / re-calc place?
            # For now, we'll just set place to none if it matches an existing place
            for v in values:
                if v['place'] == place:
                    place = None
                    break
        else:
            values = []
        values.append({"value": value, "language": language,
                       "authority": authority, "confidence": confidence, "place": place})
        self.metadata[field] = values

        # Return this as an easy way for caller to inspect or use
        return self

    def clear_metadata(self, field=None, value=None):
        if field is None:
            self.metadata = {}
        elif field in self.metadata:
            if value is None:
                self.metadata.pop(field)
            else:
                updated = []
                for v in self.metadata[field]:
                    if v != value:
                        updated.append(v)
                self.metadata[field] = updated

    def as_dict(self):
        """
        Return custom dict of this DSpaceObject with specific attributes included (no _links, etc.)
        @return: dict of this DSpaceObject for API use
        """
        return {
            'uuid': self.uuid,
            'name': self.name,
            'handle': self.handle,
            'metadata': self.metadata,
            'lastModified': self.lastModified,
            'type': self.type,
        }

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=None)

    def to_json_pretty(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class SimpleDSpaceObject(DSpaceObject):
    """
    Objects that share similar simple API methods eg. PUT update for full metadata replacement, can have handles, etc.
    By default this is Item, Community, Collection classes
    """


class Item(SimpleDSpaceObject):
    """
    Extends DSpaceObject to implement specific attributes and functions for items
    """
    type = 'item'
    inArchive = False
    discoverable = False
    withdrawn = False
    metadata = {}

    def __init__(self, api_resource=None, dso=None):
        """
        Default constructor. Call DSpaceObject init then set item-specific attributes
        @param api_resource: API result object to use as initial data
        """
        if dso is not None:
            api_resource = dso.as_dict()
            super().__init__(dso=dso)
        else:
            super().__init__(api_resource)

        if api_resource is not None:
            self.type = 'item'
            self.inArchive = api_resource['inArchive'] if 'inArchive' in api_resource else True
            self.discoverable = api_resource['discoverable'] if 'discoverable' in api_resource else False
            self.withdrawn = api_resource['withdrawn'] if 'withdrawn' in api_resource else False

    def get_metadata_values(self, field):
        """
        Return metadata values as simple list of strings
        @param field: DSpace field, eg. dc.creator
        @return: list of strings
        """
        values = []
        if field in self.metadata:
            values = self.metadata[field]
        return values

    def as_dict(self):
        """
        Return a dict representation of this Item, based on super with item-specific attributes added
        @return: dict of Item for API use
        """
        dso_dict = super().as_dict()
        item_dict = {'inArchive': self.inArchive, 'discoverable': self.discoverable, 'withdrawn': self.withdrawn}
        return {**dso_dict, **item_dict}

    @classmethod
    def from_dso(cls, dso: DSpaceObject):
        # Create new Item and copy everything over from this dso
        item = cls()
        for key, value in dso.__dict__.items():
            item.__dict__[key] = value
        return item


class Community(SimpleDSpaceObject):
    """
    Extends DSpaceObject to implement specific attributes and functions for communities
    """
    type = 'community'

    def __init__(self, api_resource=None):
        """
        Default constructor. Call DSpaceObject init then set item-specific attributes
        @param api_resource: API result object to use as initial data
        """
        super().__init__(api_resource)
        self.type = 'community'

    def as_dict(self):
        """
        Return a dict representation of this Community, based on super with community-specific attributes added
        @return: dict of Item for API use
        """
        dso_dict = super().as_dict()
        # TODO: More community-specific stuff
        community_dict = {}
        return {**dso_dict, **community_dict}


class Collection(SimpleDSpaceObject):
    """
    Extends DSpaceObject to implement specific attributes and functions for collections
    """
    type = 'collection'

    def __init__(self, api_resource=None):
        """
        Default constructor. Call DSpaceObject init then set collection-specific attributes
        @param api_resource: API result object to use as initial data
        """
        super().__init__(api_resource)
        self.type = 'collection'

    def as_dict(self):
        dso_dict = super().as_dict()
        """
        Return a dict representation of this Collection, based on super with collection-specific attributes added
        @return: dict of Item for API use
        """
        collection_dict = {}
        return {**dso_dict, **collection_dict}


class Bundle(DSpaceObject):
    """
    Extends DSpaceObject to implement specific attributes and functions for bundles
    """
    type = 'bundle'

    def __init__(self, api_resource=None):
        """
        Default constructor. Call DSpaceObject init then set bundle-specific attributes
        @param api_resource: API result object to use as initial data
        """
        super().__init__(api_resource)
        self.type = 'bundle'

    def as_dict(self):
        """
        Return a dict representation of this Bundle, based on super with bundle-specific attributes added
        @return: dict of Bundle for API use
        """
        dso_dict = super().as_dict()
        bundle_dict = {}
        return {**dso_dict, **bundle_dict}


class Bitstream(DSpaceObject):
    """
    Extends DSpaceObject to implement specific attributes and functions for bundles
    """
    type = 'bitstream'
    # Bitstream has a few extra fields specific to file storage
    bundleName = None
    sizeBytes = None
    checkSum = {
        'checkSumAlgorithm': 'MD5',
        'value': None
    }
    sequenceId = None

    def __init__(self, api_resource=None):
        """
        Default constructor. Call DSpaceObject init then set bitstream-specific attributes
        @param api_resource: API result object to use as initial data
        """
        super().__init__(api_resource)
        self.type = 'bitstream'
        if 'bundleName' in api_resource:
            self.bundleName = api_resource['bundleName']
        if 'sizeBytes' in api_resource:
            self.sizeBytes = api_resource['sizeBytes']
        if 'checkSum' in api_resource:
            self.checkSum = api_resource['checkSum']
        if 'sequenceId' in api_resource:
            self.sequenceId = api_resource['sequenceId']

    def as_dict(self):
        """
        Return a dict representation of this Bitstream, based on super with bitstream-specific attributes added
        @return: dict of Bitstream for API use
        """
        dso_dict = super().as_dict()
        bitstream_dict = {'bundleName': self.bundleName, 'sizeBytes': self.sizeBytes, 'checkSum': self.checkSum,
                          'sequenceId': self.sequenceId}
        return {**dso_dict, **bitstream_dict}

class BitstreamFormat(AddressableHALResource):
    """
    Bitstream format: https://github.com/DSpace/RestContract/blob/main/bitstreamformats.md
    example:
        {
          "shortDescription": "XML",
          "description": "Extensible Markup Language",
          "mimetype": "text/xml",
          "supportLevel": "KNOWN",
          "internal": false,
          "extensions": [
                  "xml"
          ],
          "type": "bitstreamformat"
        }
    """
    shortDescription = None
    description = None
    mimetype = None
    supportLevel = None
    internal = False
    extensions = []
    type = 'bitstreamformat'

    def __init__(self, api_resource):
        super(BitstreamFormat, self).__init__(api_resource)
        if 'shortDescription' in api_resource:
            self.shortDescription = api_resource['shortDescription']
        if 'description' in api_resource:
            self.description = api_resource['description']
        if 'mimetype' in api_resource:
            self.mimetype = api_resource['mimetype']
        if 'supportLevel' in api_resource:
            self.supportLevel = api_resource['supportLevel']
        if 'internal' in api_resource:
            self.internal = api_resource['internal']
        if 'extensions' in api_resource:
            self.extensions = api_resource['extensions'].copy()

    def as_dict(self):
        parent_dict = super(BitstreamFormat, self).as_dict()
        dict = {
            'shortDescription': self.shortDescription,
            'description': self.description,
            'mimetype': self.mimetype,
            'supportLevel': self.supportLevel,
            'internal': self.internal,
            'extensions': self.extensions,
            'type': self.type
        }
        return {**parent_dict, **dict}

class Group(DSpaceObject):
    """
    Extends DSpaceObject to implement specific attributes and methods for groups (aka. EPersonGroups)
    """
    type = 'group'
    name = None
    permanent = False

    def __init__(self, api_resource=None):
        """
        Default constructor. Call DSpaceObject init then set group-specific attributes
        @param api_resource: API result object to use as initial data
        """
        super().__init__(api_resource)
        self.type = 'group'
        if 'name' in api_resource:
            self.name = api_resource['name']
        if 'permanent' in api_resource:
            self.permanent = api_resource['permanent']

    def as_dict(self):
        """
        Return a dict representation of this Group, based on super with group-specific attributes added
        @return: dict of Group for API use
        """
        dso_dict = super().as_dict()
        group_dict = {'name': self.name, 'permanent': self.permanent}
        return {**dso_dict, **group_dict}


class User(SimpleDSpaceObject):
    """
    Extends DSpaceObject to implement specific attributes and methods for users (aka. EPersons)
    """
    type = 'user'
    name = None
    netid = None
    lastActive = None
    canLogIn = False
    email = None
    requireCertificate = False
    selfRegistered = False

    def __init__(self, api_resource=None):
        """
        Default constructor. Call DSpaceObject init then set user-specific attributes
        @param api_resource: API result object to use as initial data
        """
        super().__init__(api_resource)
        self.type = 'user'
        if 'name' in api_resource:
            self.name = api_resource['name']
        if 'netid' in api_resource:
            self.netid = api_resource['netid']
        if 'lastActive' in api_resource:
            self.lastActive = api_resource['lastActive']
        if 'canLogIn' in api_resource:
            self.canLogIn = api_resource['canLogIn']
        if 'email' in api_resource:
            self.email = api_resource['email']
        if 'requireCertificate' in api_resource:
            self.requireCertificate = api_resource['requireCertificate']
        if 'selfRegistered' in api_resource:
            self.selfRegistered = api_resource['selfRegistered']

    def as_dict(self):
        """
        Return a dict representation of this User, based on super with user-specific attributes added
        @return: dict of User for API use
        """
        dso_dict = super().as_dict()
        user_dict = {'name': self.name, 'netid': self.netid, 'lastActive': self.lastActive, 'canLogIn': self.canLogIn,
                     'email': self.email, 'requireCertificate': self.requireCertificate,
                     'selfRegistered': self.selfRegistered}
        return {**dso_dict, **user_dict}

class InProgressSubmission(AddressableHALResource):
    lastModified = None
    step = None
    sections = {}
    type = None

    def __init__(self, api_resource):
        super().__init__(api_resource)
        if 'lastModified' in api_resource:
            self.lastModified = api_resource['lastModified']
        if 'step' in api_resource:
            self.step = api_resource['lastModified']
        if 'sections' in api_resource:
            self.sections = api_resource['sections'].copy()
        if 'type' in api_resource:
            self.lastModified = api_resource['lastModified']

    def as_dict(self):
        parent_dict = super().as_dict()
        dict = {
            'lastModified': self.lastModified,
            'step': self.step,
            'sections': self.sections,
            'type': self.type
        }
        return {**parent_dict, **dict}

class WorkspaceItem(InProgressSubmission):

    def __init__(self, api_resource):
        super().__init__(api_resource)

    def as_dict(self):
        return super().as_dict()

class EntityType(AddressableHALResource):
    """
    Extends Addressable HAL Resource to model an entity type (aka item type)
    used in entities and relationships. For example, Publication, Person, Project and Journal
    are all common entity types used in DSpace 7+
    """
    def __init__(self, api_resource):
        super().__init__(api_resource)
        if 'label' in api_resource:
            self.label = api_resource['label']
        if 'type' in api_resource:
            self.label = api_resource['type']

class RelationshipType(AddressableHALResource):
    """
    TODO: RelationshipType
    """
    def __init__(self, api_resource):
        super().__init__(api_resource)

class SearchResult(HALResource):
    """
    {
      "query":"my query",
      "scope":"9076bd16-e69a-48d6-9e41-0238cb40d863",
      "appliedFilters": [
          {
            "filter" : "title",
            "operator" : "notcontains",
            "value" : "abcd",
            "label" : "abcd"
          },
          {
            "filter" : "author",
            "operator" : "authority",
            "value" : "1234",
            "label" : "Smith, Donald"
          }
      ],
      "sort" : {
        "by" : "dc.date.issued",
        "order" : "asc"
      },
      "_embedded" : {
        "searchResults": {
          "_embedded": {
            "objects" : [...],
            },

          "_links": {
            "first": {
              "href": "/api/discover/search/objects?query=my+query&scope=9076bd16-e69a-48d6-9e41-0238cb40d863&f.title=abcd,notcontains&f.author=1234,authority&page=0&size=5"
            },
            "self": {
              "href": "/api/discover/search/objects?query=my+query&scope=9076bd16-e69a-48d6-9e41-0238cb40d863&f.title=abcd,notcontains&f.author=1234,authority&page=0&size=5"
            },
            "next": {
              "href": "/api/discover/search/objects?query=my+query&scope=9076bd16-e69a-48d6-9e41-0238cb40d863&f.title=abcd,notcontains&f.author=1234,authority&page=1&size=5"
            },
            "last": {
              "href": "/api/discover/search/objects?query=my+query&scope=9076bd16-e69a-48d6-9e41-0238cb40d863&f.title=abcd,notcontains&f.author=1234,authority&page=2&size=5"
            }
          },
          "page": {
            "number": 0,
            "size": 20,
            "totalElements": 12,
            "totalPages": 3
          }
        }, "facets"... (TODO)
    """
    query = None
    scope = None
    appliedFilters = [] 
    type = None

    def __init__(self, api_resource):
        super().__init__(api_resource)
        if 'lastModified' in api_resource:
            self.lastModified = api_resource['lastModified']
        if 'step' in api_resource:
            self.step = api_resource['step']
        if 'sections' in api_resource:
            self.sections = api_resource['sections'].copy()
        if 'type' in api_resource and self.type is not None:
            self.type = api_resource['type']

    def as_dict(self):
        parent_dict = super().as_dict()
        dict = {
            'lastModified': self.lastModified,
            'step': self.step,
            'sections': self.sections,
            'type': self.type
        }
        return {**parent_dict, **dict}

class DSpaceServerError(Exception):
    """Exception raised when DSpace returns a 500 Internal Server Error response"""
    def __init__(self, message, status=500, error=None, timestamp=None, path=None):
        self.status = status
        self.timestamp = timestamp
        self.path = path
        self.message = message
        self.error = error
        super().__init__(self.format_message())

    def format_message(self):
        parts = [f"DSpace Server Error ({self.status} {self.error})"]
        if self.timestamp:
            parts.append(f"Time: {self.timestamp}")
        if self.path:
            parts.append(f"Path: {self.path}")
        parts.append(f"Message: {self.message}")
        return " | ".join(parts)
