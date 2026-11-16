//! Minimal object and method doubles shared by core protocol tests.
use std::marker::PhantomData;

use super::SetCreate;
use super::get::GetObject;
use super::method::JmapMethod;
use super::query::QueryObject;
use super::request::CallHandle;

// -- Minimal test types --

mod test_marker {
    pub(crate) enum TestObj {}
}
pub(crate) type TestObjId = crate::core::id::Id<test_marker::TestObj>;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TestObj {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub(crate) struct TestObjCreate {
    #[serde(skip)]
    _create_id: Option<crate::core::creation::CreationId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub(crate) struct TestObjPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub(crate) enum TestProp {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "name")]
    Name,
}

impl std::fmt::Display for TestProp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestProp::Id => write!(f, "id"),
            TestProp::Name => write!(f, "name"),
        }
    }
}

impl super::Object for TestObj {
    super::method::object_metadata!("Test", crate::DataType::Core, crate::core::capability::Core);
    type Property = TestProp;
    type Id = TestObjId;
    fn requires_account_id() -> bool {
        true
    }
}

impl GetObject for TestObj {
    type GetArguments = ();
}

impl super::set::SetObject for TestObj {
    type Create = TestObjCreate;
    type Patch = TestObjPatch;
    type SetArguments = ();
}

impl SetCreate for TestObjCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.as_ref().map(ToString::to_string)
    }
    fn set_create_id(&mut self, id: crate::core::creation::CreationId) {
        self._create_id = Some(id);
    }
    fn new(create_id: Option<crate::core::creation::CreationId>) -> Self {
        TestObjCreate {
            _create_id: create_id,
            name: None,
        }
    }
}

impl super::changes::ChangesObject for TestObj {
    type ChangesResponse = ();
}

impl QueryObject for TestObj {
    type QueryArguments = ();
    type Filter = ();
    type Sort = ();
}

crate::define_get_method!(TestGet, TestObj);
crate::define_query_method!(TestQuery, TestObj);
// A second get method on a DIFFERENT capability, so the `using`
// accumulation in `Request::call` has something to accumulate. Every
// other test method rides `Core`, which `Request::new` already seeds.
#[derive(serde::Serialize)]
pub(crate) struct TestMailGet(TestGet);

impl TestMailGet {
    pub(crate) fn new() -> Self {
        Self(TestGet::new())
    }
}

impl JmapMethod for TestMailGet {
    const NAME: &'static str = "TestMail/get";
    const REPLAY_SAFE: bool = true;
    type Cap = crate::core::capability::Mail;
    type Response = <TestGet as JmapMethod>::Response;

    fn stamp_response(response: &mut Self::Response, context: crate::core::answer::AnswerContext) {
        response.answer_context = context;
    }

    fn set_account_id(&mut self, account_id: &crate::core::id::AccountId) {
        self.0.set_account_id(account_id);
    }
}

pub(crate) fn make_handle<M: JmapMethod>(call_id: &str) -> CallHandle<M> {
    CallHandle {
        call_id: call_id.to_string(),
        answer_context: crate::core::answer::AnswerContext {
            method: M::NAME,
            guarded: false,
            write: !M::REPLAY_SAFE,
        },
        _phantom: PhantomData,
    }
}
