use std::any::type_name;

pub(crate) fn type_name_short<'a, T>() -> &'a str {
    let s_name = type_name::<T>().split('<').collect::<Vec<_>>().first().unwrap().split("::").collect::<Vec<_>>();
    *s_name.last().unwrap()
}
