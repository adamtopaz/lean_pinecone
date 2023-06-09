import LeanPinecone.Basic

open Lean

structure DataPoint where
  name : String
  type : String
  module : String
  rev : String
  nameHash : UInt64
  typeHash : UInt64
  nameEmb : Option (Array JsonNumber)
  typeEmb : Option (Array JsonNumber)
deriving ToJson, FromJson

def main : IO Unit := IO.FS.withFile "../embeddings.jsonl" .read fun handle => do
  let mut line : String := "START"
  let mut batch : Array DataPoint := #[]
  while true do
    line ← handle.getLine
    if line == "" then break
    let .ok json := Json.parse line | throw <| .userError "Failed to parse json"
    let .ok (dataPoint : DataPoint) := fromJson? json | throw <| .userError "Failed to parse datapoint"
    batch := batch.push dataPoint
    if batch.size == 100 then 
      process batch
      batch := #[]
  process batch
where
process (batch) := do
  let nameBatch : Array Pinecone.Element := 
    batch.filter (fun b => !b.nameEmb.isNone) |>.map fun b => 
    { id := toString b.nameHash
      values := b.nameEmb.get!
      metadata := some <| Json.mkObj [
        ("name", b.name),
        ("type", b.type),
        ("module", b.module),
        ("rev", b.rev),
        ("nameHash", toJson b.nameHash),
        ("typeHash", toJson b.typeHash)
      ] }
  let typeBatch : Array Pinecone.Element := 
    batch.filter (fun b => !b.typeEmb.isNone) |>.map fun b => 
    { id := toString b.nameHash
      values := b.typeEmb.get!
      metadata := some <| Json.mkObj [
        ("name", b.name),
        ("type", b.type),
        ("module", b.module),
        ("rev", b.rev),
        ("nameHash", toJson b.nameHash),
        ("typeHash", toJson b.typeHash)
      ] }
  let out ← PineconeM.upsert nameBatch "name" |>.run
  IO.println s!"Uploaded {out} for name batch of size {nameBatch.size}"
  let out ← PineconeM.upsert typeBatch "type" |>.run
  IO.println s!"Uploaded {out} for type batch of size {typeBatch.size}"