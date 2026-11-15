pub(crate) type ContactCardParseResponse =
    crate::core::parse::ParseResponse<Vec<super::ContactCard>>;

crate::define_parse_method!(
    ContactCardParseRequest,
    super::ContactCard,
    crate::core::capability::ContactsParse,
    ContactCardParseResponse
);
