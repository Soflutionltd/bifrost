use super::{ACL, DKIM, PrincipalAccount, PrincipalCreate, PrincipalId, PrincipalPatch, Type};
use crate::core::field::Field;
use crate::core::id::AccountId;
use crate::core::patch::{self, PatchPath};
use std::collections::HashMap;

macro_rules! principal_setters {
    ($t:ty) => {
        impl $t {
            pub(crate) fn name(&mut self, name: impl Into<String>) -> &mut Self {
                self.name = Field::Value(name.into());
                self
            }

            pub(crate) fn description(
                &mut self,
                description: Option<impl Into<String>>,
            ) -> &mut Self {
                self.description = Field::from(description.map(std::convert::Into::into));
                self
            }

            pub(crate) fn email(&mut self, email: impl Into<String>) -> &mut Self {
                self.email = Field::Value(email.into());
                self
            }

            pub(crate) fn secret(&mut self, secret: impl Into<String>) -> &mut Self {
                self.secret = Field::Value(secret.into());
                self
            }

            pub(crate) fn timezone(&mut self, timezone: Option<impl Into<String>>) -> &mut Self {
                self.timezone = Field::from(timezone.map(std::convert::Into::into));
                self
            }

            pub(crate) fn picture(&mut self, picture: Option<impl Into<String>>) -> &mut Self {
                self.picture = Field::from(picture.map(std::convert::Into::into));
                self
            }

            pub(crate) fn quota(&mut self, quota: Option<u32>) -> &mut Self {
                self.quota = quota;
                self
            }

            pub(crate) fn ptype(&mut self, ptype: Type) -> &mut Self {
                self.ptype = ptype.into();
                self
            }

            pub(crate) fn dkim(&mut self, dkim: DKIM) -> &mut Self {
                self.dkim = dkim.into();
                self
            }

            pub(crate) fn acl(&mut self, acl: Option<HashMap<PrincipalId, Vec<ACL>>>) -> &mut Self {
                self.acl = Field::from(acl);
                self
            }

            pub(crate) fn aliases<T, U>(&mut self, aliases: Option<T>) -> &mut Self
            where
                T: IntoIterator<Item = U>,
                U: Into<String>,
            {
                self.clear_membership_patch("aliases");
                self.aliases = Field::from(
                    aliases.map(|l| l.into_iter().map(std::convert::Into::into).collect()),
                );
                self
            }

            pub(crate) fn capabilities(
                &mut self,
                capabilities: Option<HashMap<String, serde_json::Value>>,
            ) -> &mut Self {
                self.capabilities = capabilities;
                self
            }

            pub(crate) fn accounts(
                &mut self,
                accounts: Option<HashMap<AccountId, PrincipalAccount>>,
            ) -> &mut Self {
                self.accounts = accounts;
                self
            }

            pub(crate) fn members<T, U>(&mut self, members: Option<T>) -> &mut Self
            where
                T: IntoIterator<Item = U>,
                U: Into<PrincipalId>,
            {
                self.clear_membership_patch("members");
                self.members = Field::from(
                    members.map(|l| l.into_iter().map(std::convert::Into::into).collect()),
                );
                self
            }
        }
    };
}

principal_setters!(PrincipalCreate);
principal_setters!(PrincipalPatch);

impl PrincipalCreate {
    fn clear_membership_patch(&mut self, _property: &str) {
        // Create objects carry complete properties, never PatchObject paths.
    }
}

impl PrincipalPatch {
    fn clear_membership_patch(&mut self, property: &str) {
        if let Some(entries) = &mut self.property_patch {
            patch::remove(entries, property);
        }
    }

    pub(crate) fn alias(&mut self, alias: &str, set: bool) -> &mut Self {
        // Latest setter wins: an enterprise administrator expects an alias
        // edit to supersede an earlier replacement, never an invalid overlap.
        self.aliases = Field::Omitted;
        patch::insert(
            self.property_patch.get_or_insert_with(HashMap::new),
            PatchPath::new(["aliases", alias]),
            set,
        );
        self
    }

    pub(crate) fn member(&mut self, member: &str, set: bool) -> &mut Self {
        self.members = Field::Omitted;
        patch::insert(
            self.property_patch.get_or_insert_with(HashMap::new),
            PatchPath::new(["members", member]),
            set,
        );
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn membership_paths_escape_tokens_and_keep_siblings() {
        let mut patch = PrincipalPatch::default();
        patch.alias("a/b~c", true).alias("other", false);
        patch.member("m/n~o", false).member("peer", true);
        assert_eq!(
            serde_json::to_value(patch).unwrap(),
            json!({
                "aliases/a~1b~0c": true, "aliases/other": false,
                "members/m~1n~0o": false, "members/peer": true
            })
        );
    }

    #[test]
    fn latest_membership_setter_supersedes_overlapping_edits() {
        let mut patch = PrincipalPatch::default();
        patch.aliases(Some(["old"])).members(Some(["old"]));
        patch.alias("new", true).member("new", true);
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"aliases/new": true, "members/new": true})
        );
        patch.aliases(Some(["whole"])).members(Some(["whole"]));
        assert_eq!(
            serde_json::to_value(&patch).unwrap(),
            json!({"aliases": ["whole"], "members": ["whole"]})
        );
        patch.alias("last", true).member("last", true);
        patch
            .aliases(None::<Vec<String>>)
            .members(None::<Vec<PrincipalId>>);
        assert_eq!(
            serde_json::to_value(patch).unwrap(),
            json!({"aliases": null, "members": null})
        );
    }
}
