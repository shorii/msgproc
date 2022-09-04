use std::any::Any;
use std::collections::HashMap;

pub(crate) struct AnyOptions {
    options: HashMap<String, Box<dyn Any>>,
}

impl AnyOptions {
    pub fn new() -> Self {
        Self {
            options: HashMap::new(),
        }
    }

    pub fn get<T>(&self, key: &str) -> T
    where
        T: Any + Copy,
    {
        *self.options.get(key).unwrap().downcast_ref::<T>().unwrap()
    }

    pub fn set<T>(&mut self, key: &str, value: T)
    where
        T: Any + Copy,
    {
        match self.options.get_mut(key) {
            Some(any_value) => {
                let downcasted = any_value.downcast_mut::<T>().unwrap();
                *downcasted = value;
            }
            None => {
                self.options.insert(key.to_string(), Box::new(value));
            }
        }
    }
}
