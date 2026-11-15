use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Person {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resource_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) names: Option<Vec<Name>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) email_addresses: Option<Vec<EmailAddress>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) phone_numbers: Option<Vec<PhoneNumber>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) addresses: Option<Vec<PeopleAddress>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) organizations: Option<Vec<Organization>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) photos: Option<Vec<Photo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) biographies: Option<Vec<Biography>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) memberships: Option<Vec<Membership>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PeopleConnectionsResponse {
    pub(crate) connections: Option<Vec<Person>>,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OtherContactsResponse {
    pub(crate) other_contacts: Option<Vec<Person>>,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UpdateContactPhotoRequest {
    pub(crate) photo_bytes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ContactGroupsResponse {
    pub(crate) contact_groups: Option<Vec<ContactGroup>>,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ContactGroup {
    pub(crate) resource_name: Option<String>,
    pub(crate) name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PeopleSearchResponse {
    pub(crate) results: Option<Vec<SearchResult>>,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SearchResult {
    pub(crate) person: Person,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DirectoryPeopleResponse {
    pub(crate) people: Option<Vec<Person>>,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Name {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) given_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) family_name: Option<String>,
    #[serde(flatten)]
    pub(crate) extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EmailAddress {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) value: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) formatted_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metadata: Option<FieldMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PhoneNumber {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) value: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) formatted_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metadata: Option<FieldMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PeopleAddress {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) formatted_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) street_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) city: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) postal_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) country: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) formatted_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metadata: Option<FieldMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FieldMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) primary: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Organization {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) department: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Photo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Biography {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Membership {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) contact_group_membership: Option<ContactGroupMembership>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ContactGroupMembership {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) contact_group_resource_name: Option<String>,
}
