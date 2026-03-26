use crate::config::TransformConfig;
use crate::sources::LogEvent;
use regex::Regex;

pub fn apply(transforms: &[TransformConfig], event: &mut LogEvent) -> bool {
    for t in transforms {
        match t {
            TransformConfig::Filter { include, exclude } => {
                if let Some(p) = include { if let Ok(re) = Regex::new(p) { if !re.is_match(&event.message) { return false; } } }
                if let Some(p) = exclude { if let Ok(re) = Regex::new(p) { if re.is_match(&event.message) { return false; } } }
            }
            TransformConfig::Parse { format, pattern } => {
                match format.as_str() {
                    "json" => parse_json(event),
                    "logfmt" => parse_logfmt(event),
                    "regex" => { if let Some(p) = pattern { parse_regex(event, p); } }
                    _ => {}
                }
            }
            TransformConfig::Mask { fields } => {
                for rule in fields {
                    if let Ok(re) = Regex::new(&rule.pattern) {
                        event.message = re.replace_all(&event.message, rule.replacement.as_str()).to_string();
                        for fname in &rule.fields {
                            if let Some(v) = event.fields.get_mut(fname) {
                                *v = re.replace_all(v, rule.replacement.as_str()).to_string();
                            }
                        }
                    }
                }
            }
            TransformConfig::AddFields { fields } => {
                for (k, v) in fields { event.fields.insert(k.clone(), v.clone()); }
            }
        }
    }
    true
}

fn parse_json(event: &mut LogEvent) {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&event.message) {
        if let Some(obj) = v.as_object() {
            for (k, v) in obj {
                match v {
                    serde_json::Value::String(s) => { event.fields.insert(k.clone(), s.clone()); }
                    other => { event.fields.insert(k.clone(), other.to_string()); }
                }
            }
            if let Some(msg) = obj.get("message").or(obj.get("msg")) {
                if let Some(s) = msg.as_str() { event.message = s.to_string(); }
            }
        }
    }
}

fn parse_logfmt(event: &mut LogEvent) {
    for part in event.message.split_whitespace() {
        if let Some((k, v)) = part.split_once('=') {
            event.fields.insert(k.to_string(), v.trim_matches('"').to_string());
        }
    }
}

fn parse_regex(event: &mut LogEvent, pattern: &str) {
    if let Ok(re) = Regex::new(pattern) {
        if let Some(caps) = re.captures(&event.message) {
            for name in re.capture_names().flatten() {
                if let Some(m) = caps.name(name) { event.fields.insert(name.to_string(), m.as_str().to_string()); }
            }
        }
    }
}
