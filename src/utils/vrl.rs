// src/vrl.rs
use std::path::{Path, PathBuf};

use log::warn;
use serde_json::Value;
use vrl::{
    compiler::{
        self,
        state::RuntimeState,
        Context as VrlContext,
        Program as VrlProgram,
        TargetValue,
        TimeZone as VrlTimeZone,
    },
    diagnostic::{Diagnostic, DiagnosticList, Formatter as DiagFormatter},
    value::{Secrets, Value as VrlValue},
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock}; 
static VRL_CACHE: OnceLock<Mutex<HashMap<PathBuf, Arc<VrlProgram>>>> = OnceLock::new();

/// Public alias so json2splunk.rs can use it.
pub type VrlChain = Vec<(VrlProgram, String)>;

/// Compile a list of VRL scripts into a chain.
///
/// `vrl_dir`:
///   - Some(dir): paths in `normalize_paths` are resolved relative to this directory
///   - None: paths are used as-is (absolute or relative to CWD)
pub fn compile_vrl_chain(vrl_dir: Option<&Path>, normalize_paths: &[String]) -> VrlChain {
    if normalize_paths.is_empty() {
        return Vec::new();
    }

    // Initialize the cache if it hasn't been accessed yet
    let cache_lock = VRL_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    
    let functions = vrl::stdlib::all();
    let mut out = Vec::new();

    for p in normalize_paths {
        let path: PathBuf = if let Some(base) = vrl_dir {
            base.join(p)
        } else {
            PathBuf::from(p)
        };

        // 1. Check Cache
        {
            let cache = cache_lock.lock().unwrap();
            if let Some(program) = cache.get(&path) {
                // If found, clone the Arc (cheap) and continue
                // We don't store the source string in cache for simplicity, 
                // but we need it for the tuple. We'll pass a placeholder or read it lightly.
                out.push((program.as_ref().clone(), p.clone())); 
                continue;
            }
        } // Drop lock here to do IO

        // 2. Compile if not in cache
        let source = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) => {
                log::error!("Cannot read VRL file {}: {}. Skipping.", path.display(), e);
                continue;
            }
        };

        match compiler::compile(&source, &functions) {
            Ok(compiled) => {
                let prog_arc = Arc::new(compiled.program.clone());
                
                // 3. Store in Cache
                {
                    let mut cache = cache_lock.lock().unwrap();
                    cache.insert(path.clone(), prog_arc);
                }

                out.push((compiled.program, source));
            }
            Err(diags) => {
                let diags_list = DiagnosticList::from(diags);
                let fmt = DiagFormatter::new(&source, diags_list).colored();
                warn!("Failed to compile VRL {}:\n{}", path.display(), fmt);
            }
        }
    }

    out
}

/// Apply a VRL chain to a serde_json `Value`.
/// Returns:
///   - Some(new_value) on success
///   - None if any program in the chain fails at runtime
pub fn apply_vrl_chain_to_record(val: Value, chain: &[(VrlProgram, String)]) -> Option<Value> {
    if chain.is_empty() {
        return Some(val);
    }

    let mut vrl_val: VrlValue = serde_json::from_value(val).ok()?;
    let tz = VrlTimeZone::default();

    for (prog, src) in chain {
        let mut target = TargetValue {
            value: vrl_val,
            metadata: VrlValue::Object(Default::default()),
            secrets: Secrets::default(),
        };
        let mut state = RuntimeState::default();
        let mut ctx = VrlContext::new(&mut target, &mut state, &tz);

        if let Err(err) = prog.resolve(&mut ctx) {
            let diag: Diagnostic = err.into();
            let diag_list = DiagnosticList::from(vec![diag]);
            let formatter = DiagFormatter::new(src, diag_list).colored();
            warn!("VRL runtime error:\n{}", formatter);
            return None;
        }

        vrl_val = target.value;
    }

    serde_json::to_value(&vrl_val).ok()
}
