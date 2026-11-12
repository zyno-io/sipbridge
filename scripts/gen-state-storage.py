#!/usr/bin/env python3
"""Generate typed storage-only protobuf schemas/adapters from the retained Go graph.

Go reflection runs only in the generator test. Runtime codecs use generated typed
conversions, never JSON or reflection-based object mapping. Field numbers persist
in the checked-in registry; removed fields remain reserved.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import shutil
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
OUTPUTS = ["proto/sipbridge-storage.proto", "proto/sipbridge-storage.fields.json", "internal/storagepb/sipbridge-storage.pb.go", "internal/engine/storage_generated.go", "internal/state/storage_generated.go", "internal/storagecodec/receipt.json", "internal/engine/storage_direct_generated.go"]
if "--check" in sys.argv:
    with tempfile.TemporaryDirectory(prefix="sipbridge-storage-generator-") as directory:
        target = Path(directory)
        for entry in ["go.mod", "go.sum", "internal", "cmd", "proto", "scripts", "dependencies", "third_party"]:
            source = ROOT / entry
            if source.is_dir(): shutil.copytree(source,target/entry)
            else: shutil.copy2(source,target/entry)
        subprocess.run([sys.executable,str(target/"scripts/gen-state-storage.py")],cwd=target,check=True)
        drift = [entry for entry in OUTPUTS if (target/entry).read_bytes() != (ROOT/entry).read_bytes()]
        if drift: raise SystemExit("storage generator drift: " + ", ".join(drift))
    print("private storage generation reproduced without modifying the checkout")
    raise SystemExit(0)
if "--direct-only" not in sys.argv:
    protoc = os.environ.get("PROTOC", "protoc")
    version = subprocess.check_output([protoc,"--version"],text=True).strip()
    if version != "libprotoc 3.21.12": raise RuntimeError("require protoc 3.21.12; got " + version)
    plugin = subprocess.check_output(["protoc-gen-go","--version"],text=True).strip()
    if plugin != "protoc-gen-go v1.36.12": raise RuntimeError("require protoc-gen-go 1.36.12; got " + plugin)
env = dict(os.environ, SIPBRIDGE_GENERATE_STORAGE="1")
result = subprocess.run(["go", "test", "./internal/engine", "-run", "^TestGenerateStorageTypes$", "-count=1", "-v"], env=env, check=True, capture_output=True, text=True)
raw = result.stdout.split("STORAGE_TYPES ", 1)[1]
types, _ = json.JSONDecoder().raw_decode(raw)
registry_file = ROOT / "proto/sipbridge-storage.fields.json"
registry = json.loads(registry_file.read_text()) if registry_file.exists() else {}

def name(t):
    item = types[t]
    prefix = {"sipbridge/internal/state": "State", "sipbridge/internal/engine": "Engine", "github.com/emiago/sipgo/sip/portable": "Portable"}[item["package"]]
    return prefix + item["name"][0].upper() + item["name"][1:]

def field_name(native):
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", native)).lower()

def go(t, owner):
    if t.startswith("*"): return "*" + go(t[1:], owner)
    if t.startswith("[]"): return "[]" + go(t[2:], owner)
    if t.startswith("map["):
        key, val = t[4:].split("]", 1)
        return "map[" + go(key, owner) + "]" + go(val, owner)
    if t.startswith("pb:"): return "pb." + t.split(":")[2]
    if t.startswith("enum:"): return "pb." + t.split(":")[1]
    if t not in types: return t
    item = types[t]
    if item["package"] == "github.com/emiago/sipgo/sip/portable": return "portable." + item["name"]
    if item["package"] == "sipbridge/internal/" + owner: return item["name"]
    raise ValueError((t, owner))

def scalar(t):
    if t.startswith("*"): return scalar(t[1:])
    if t == "[]uint8": return "bytes"
    if t.startswith("pb:"): return "." + t.split(":")[1]
    if t.startswith("enum:"): return ".sipbridge.control.v1." + t.split(":")[1].replace("_", ".", 1)
    if t in types:
        if types[t]["kind"] == "struct": return name(t)
        return scalar(types[t]["kind"])
    return {"string":"string", "bool":"bool", "uint8":"uint32", "uint16":"uint32", "uint32":"uint32", "uint64":"uint64", "int":"int64", "int64":"int64", "int32":"int32"}[t]

def pbgo(t):
    if t.startswith("*"): return pbgo(t[1:])
    if t.startswith("pb:"): return "*pb." + t.split(":")[2]
    if t.startswith("enum:"): return "pb." + t.split(":")[1]
    if t in types:
        if types[t]["kind"] == "struct": return "*storagepb." + name(t)
        return pbgo(types[t]["kind"])
    return {"int":"int64", "uint8":"uint32", "uint16":"uint32"}.get(t, t)

def expr(t, value, owner, direction):
    if t.startswith("*"):
        inner = t[1:]
        if inner.startswith("pb:"): return value
        raise ValueError("pointer expression requires block")
    if t in types and types[t]["kind"] == "struct":
        return f"storage{direction}{name(t)}({value})"
    target = pbgo(t) if direction == "Encode" else go(t, owner)
    return f"{target}({value})" if pbgo(t) != go(t,owner) or direction == "Decode" and t in types else value

# The direct reader consumes the same reflected fields and stable registry, never
# a hand-maintained schema catalog. Direct-only mode leaves the other outputs alone.
from gen_state_storage_direct import generate_direct_group_reader

if "--direct-only" in sys.argv:
    direct_reader = generate_direct_group_reader(types, registry, name, go)
    path = ROOT / "internal/engine/storage_direct_generated.go"
    path.write_text(direct_reader)
    subprocess.run(["gofmt", "-w", str(path)], check=True)
    raise SystemExit(0)

schema = ['// Code generated by scripts/gen-state-storage.py. DO NOT EDIT.', 'syntax = "proto3";', 'package sipbridge.storage.v1;', 'option go_package = "sipbridge/internal/storagepb";', 'import "sipbridge-control.proto";', '']
outputs = {"state": [], "engine": []}
for t, item in sorted(types.items()):
    if item["kind"] != "struct": continue
    owner = "state" if item["package"] == "sipbridge/internal/state" else "engine"
    message = name(t)
    numbers = registry.setdefault(message, {})
    for f in item["fields"]:
        if f["name"] not in numbers: numbers[f["name"]] = max(numbers.values(), default=0) + 1
    fields = item["fields"]
    schema.append(f"message {message} {{")
    for old, num in numbers.items():
        if old not in [f["name"] for f in fields]: schema.append(f"  reserved {num}; // {old}")
    encode = [f"func storageEncode{message}(v {go(t,owner)}) *storagepb.{message} {{", f"  out := &storagepb.{message}{{}}"]
    decode = [f"func storageDecode{message}(v *storagepb.{message}) {go(t,owner)} {{", f"  var out {go(t,owner)}", "  if v == nil { return out }"]
    extras = []
    for f in fields:
        ft = f["type"]; number = numbers[f["name"]]
        proto_field = field_name(f["name"])
        pf = "".join(part[0].upper()+part[1:] for part in proto_field.split("_")); vf = "v." + f["name"]; of = "out." + f["name"]
        if ft.startswith("map[") or ft.startswith("[]") and ft != "[]uint8":
            wrapper = message + f["name"][0].upper() + f["name"][1:] + "Values"
            schema.append(f"  {wrapper} {proto_field} = {number}; // {f['name']} (nil versus empty preserved)")
            if ft.startswith("map["):
                key, val = ft[4:].split("]", 1)
                nullable = val.startswith("*")
                map_value = wrapper + "EntryValue" if nullable else scalar(val)
                map_go = "*storagepb." + map_value if nullable else pbgo(val)
                extras += [f"message {wrapper} {{", f"  map<{scalar(key)}, {map_value}> values = 1;", "}"]
                if nullable: extras += [f"message {map_value} {{", f"  {scalar(val)} value = 1;", "}"]
                encode += [f"  if {vf} != nil {{", f"    out.{pf} = &storagepb.{wrapper}{{Values: make(map[{pbgo(key)}]{map_go}, len({vf}))}}", f"    for k, value := range {vf} {{"]
                decode += [f"  if v.{pf} != nil {{", f"    {of} = make({go(ft,owner)}, len(v.{pf}.Values))", f"    for k, value := range v.{pf}.Values {{"]
                ek = expr(key,"k",owner,"Encode"); dk = expr(key,"k",owner,"Decode")
                dest_e=f"out.{pf}.Values[{ek}]"; dest_d=f"{of}[{dk}]"
            else:
                val=ft[2:]
                extras += [f"message {wrapper} {{", f"  repeated {scalar(val)} values = 1;", "}"]
                encode += [f"  if {vf} != nil {{", f"    out.{pf} = &storagepb.{wrapper}{{Values: make([]{pbgo(val)}, len({vf}))}}", f"    for k, value := range {vf} {{"]
                decode += [f"  if v.{pf} != nil {{", f"    {of} = make({go(ft,owner)}, len(v.{pf}.Values))", f"    for k, value := range v.{pf}.Values {{"]
                dest_e=f"out.{pf}.Values[k]"; dest_d=f"{of}[k]"
            if ft.startswith("map[") and val.startswith("*"):
                inner = val[1:]
                encode += [f"      {dest_e} = &storagepb.{map_value}{{}}"]
                if inner.startswith("pb:"):
                    encode += [f"      {dest_e}.Value = value"]
                    decode += [f"      if value != nil {{ {dest_d} = value.Value }} else {{ {dest_d} = nil }}"]
                else:
                    encode += [f"      if value != nil {{ {dest_e}.Value = storageEncode{name(inner)}(*value) }}"]
                    decode += [f"      if value != nil && value.Value != nil {{ decoded := storageDecode{name(inner)}(value.Value); {dest_d} = &decoded }} else {{ {dest_d} = nil }}"]
            elif val.startswith("*") and not val.startswith("*pb:"):
                inner = val[1:]
                encode += [f"      if value != nil {{ {dest_e} = storageEncode{name(inner)}(*value) }}"]
                decode += [f"      if value != nil {{ decoded := storageDecode{name(inner)}(value); {dest_d} = &decoded }} else {{ {dest_d} = nil }}"]
            else:
                encode += [f"      {dest_e} = {expr(val,'value',owner,'Encode')}"]
                decode += [f"      {dest_d} = {expr(val,'value',owner,'Decode')}"]
            encode += ["    }", "  }"]; decode += ["    }", "  }"]
        else:
            schema.append(f"  {scalar(ft)} {proto_field} = {number}; // {f['name']}")
            if ft.startswith("*") and not ft.startswith("*pb:"):
                inner=ft[1:]
                encode += [f"  if {vf} != nil {{ out.{pf} = storageEncode{name(inner)}(*{vf}) }}"]
                decode += [f"  if v.{pf} != nil {{ decoded := storageDecode{name(inner)}(v.{pf}); {of} = &decoded }}"]
            else:
                encode += [f"  out.{pf} = {expr(ft,vf,owner,'Encode')}"]
                decode += [f"  {of} = {expr(ft,'v.'+pf,owner,'Decode')}"]
    schema += ["}", *extras, ""]
    outputs[owner] += encode + ["  return out", "}", ""] + decode + ["  return out", "}", ""]
direct_reader = generate_direct_group_reader(types, registry, name, go)
registry_file.write_text(json.dumps(registry,indent=2,sort_keys=True)+"\n")
(ROOT/"proto/sipbridge-storage.proto").write_text("\n".join(schema))
for owner, lines in outputs.items():
    imports = ['"sipbridge/internal/storagepb"']
    if owner == "engine": imports += ['pb "sipbridge/internal/controlpb"', '"github.com/emiago/sipgo/sip/portable"']
    data = '// Code generated by scripts/gen-state-storage.py. DO NOT EDIT.\npackage '+owner+'\n\nimport (\n'+'\n'.join(imports)+'\n)\n\n'+'\n'.join(lines)
    file = ROOT / "internal" / owner / "storage_generated.go"
    file.write_text(data)
    subprocess.run(["gofmt","-w",str(file)],check=True)
protoc = os.environ.get("PROTOC", "protoc")
version = subprocess.check_output([protoc,"--version"],text=True).strip()
if version != "libprotoc 3.21.12": raise RuntimeError("require protoc 3.21.12; got " + version)
subprocess.run([protoc,"-Iproto","--go_out=.","--go_opt=module=sipbridge","proto/sipbridge-storage.proto"],check=True)

direct_path = ROOT / "internal/engine/storage_direct_generated.go"
direct_path.write_text(direct_reader)
subprocess.run(["gofmt", "-w", str(direct_path)], check=True)

subprocess.run(["python3", "scripts/verify_storage_schema.py", "--write"],check=True)
