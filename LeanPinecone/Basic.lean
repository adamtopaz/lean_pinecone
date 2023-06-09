import Lean

open Lean

namespace Pinecone

structure Element where
  metadata : Option Json
  id : String
  values : Array JsonNumber
deriving ToJson, FromJson

structure Config where
  apiKey : String
  environment : String
  project : String
  index : String
deriving ToJson, FromJson

structure UpsertResponse where
  upsertedCount : Nat
deriving ToJson, FromJson

structure UpsertError.Detail where
  typeUrl : String
  value : String
deriving ToJson, FromJson

structure Error where
  code : Nat
  message : String
  details : Array UpsertError.Detail
deriving ToJson, FromJson

structure Query where
  topK : Nat
  vector : Array JsonNumber
  nmspace : Option String := none
  includeValues : Bool := false
  includeMetadata : Bool := true
  filter : Option Json := none

instance : ToJson Query where
  toJson e := Json.mkObj [
    ("namespace", toJson e.nmspace),
    ("topK", e.topK),
    ("includeValues", e.includeValues),
    ("includeMetadata", e.includeMetadata),
    ("vector", toJson e.vector),
    ("filter", toJson e.filter)
  ]

instance : FromJson Query where
  fromJson? json := do
    let .ok nmspace := json.getObjValAs? (Option String) "namespace" | .error s!"Failed to parse {json}"
    let .ok topK := json.getObjValAs? Nat "topK" | .error s!"Failed to parse {json}"
    let .ok includeValues := json.getObjValAs? Bool "includeValues" | .error s!"Failed to parse {json}"
    let .ok includeMetadata := json.getObjValAs? Bool "includeMetadata" | .error s!"Failed to parse {json}"
    let .ok vector := json.getObjValAs? (Array JsonNumber) "vector" | .error s!"Failed to parse {json}"
    let .ok filter := json.getObjValAs? (Option Json) "filter" | .error s!"Failed to parse {json}"
    return { 
      nmspace := nmspace, 
      topK := topK,
      includeValues := includeValues,
      includeMetadata := includeMetadata,
      vector := vector
      filter := filter
    }

structure Match where
  id : String
  score : JsonNumber
  values : Option (Array JsonNumber)
  metadata : Option Json 
deriving ToJson, FromJson

structure QueryResponse where
  nmspace : String
  mtches : Array Match

instance : ToJson QueryResponse where
  toJson q := Json.mkObj [
    ("namespace", q.nmspace),
    ("matches", toJson q.mtches)
  ]

instance : FromJson QueryResponse where
  fromJson? json := do
    let .ok nmspace := json.getObjValAs? String "namespace" | .error s!"Failed to parse {json}"
    let .ok mtchs := json.getObjValAs? (Array Match) "matches" | .error s!"Failed to parse {json}"
    return ⟨nmspace, mtchs⟩

end Pinecone

open Pinecone

abbrev PineconeM := ReaderT Pinecone.Config IO

namespace PineconeM

def baseUrl : PineconeM String := do
  return s!"{(← read).index}-{(← read).project}.svc.{(← read).environment}.pinecone.io"

def upsertAux (data : Array Element) (nmspace : String) : 
    PineconeM (UInt32 × String × String) := do
  let child ← IO.Process.spawn {
    cmd := "curl"
    args := #["-X", "POST", 
    s!"https://{← baseUrl}/vectors/upsert",
    "-H", "Content-Type: application/json",
    "-H", s!"Api-Key: {(← read).apiKey}",
    "--data-binary", "@-"]
    stdin := .piped
    stdout := .piped
    stderr := .piped
  }
  let (stdin, child) ← child.takeStdin
  stdin.putStr <| toString <| toJson <| Json.mkObj [("vectors", toJson data), ("namespace", nmspace)]
  stdin.flush 
  let stdout ← IO.asTask child.stdout.readToEnd .dedicated
  let err ← child.stderr.readToEnd
  let exitCode ← child.wait
  if exitCode != 0 then 
    throw <| .userError err
  let out ← IO.ofExcept stdout.get
  return (exitCode, out, err)
  
def parseUpsertResponse (s : String) : Except String (Except Error UpsertResponse) := do
  match Json.parse s with 
    | .error e => .error s!"[PineconeM.parseUpsertResponse] Error parsing JSON: {e}\n{s}"
    | .ok json => 
      if let .ok error := fromJson? json then pure (.error error)
      else if let .ok data := fromJson? json then pure (.ok data)
      else .error s!"[PineconeM.parseUpsertResponse] Error parsing JSON:\n{s}"

def upsert (data : Array Element) (nmspace : String) : PineconeM Nat := do
  let (_, rawOut, _) ← upsertAux data nmspace
  match parseUpsertResponse rawOut with
  | .ok (.ok out) => 
    return out.upsertedCount
  | .ok (.error error) => 
    throw <| .userError s!"[PineconeM.upsert] Server error:\n{toJson error}"
  | .error error => 
    throw <| .userError s!"[PineconeM.upsert] Unable to parse server response:\n{error}"

def queryAux (query : Query) : PineconeM (UInt32 × String × String) := do
  let child ← IO.Process.spawn {
    cmd := "curl"
    args := #["-X", "POST", 
    s!"https://{← baseUrl}/query",
    "-H", "Content-Type: application/json",
    "-H", s!"Api-Key: {(← read).apiKey}",
    "--data-binary", "@-"]
    stdin := .piped
    stdout := .piped
    stderr := .piped
  }
  let (stdin, child) ← child.takeStdin
  stdin.putStr <| toString <| toJson <| query
  stdin.flush 
  let stdout ← IO.asTask child.stdout.readToEnd .dedicated
  let err ← child.stderr.readToEnd
  let exitCode ← child.wait
  if exitCode != 0 then 
    throw <| .userError err
  let out ← IO.ofExcept stdout.get
  return (exitCode, out, err)

def parseQueryResponse (s : String) : Except String (Except Error QueryResponse) := do
  match Json.parse s with 
    | .error e => .error s!"{e}\n{s}"
    | .ok json => 
      if let .ok error := fromJson? json then pure (.error error)
      else if let .ok data := fromJson? json then pure (.ok data)
      else .error s!"[PineconeM.parseQueryResponse] Error parsing:\n{s}"

def query (query : Query) : PineconeM QueryResponse := do
  let (_, rawOut, _) ← queryAux query
  match parseQueryResponse rawOut with
  | .ok (.ok out) => return out
  | .ok (.error error) => 
    throw <| .userError s!"[PineconeM.query] Server error:\n{toJson error}"
  | .error error => 
    throw <| .userError s!"[PineconeM.query] Unable to parse server response:\n{error}"

def runWith {A : Type _} (m : PineconeM A) 
    (apiKey : Option String := none)
    (project : Option String := none)
    (index : Option String := none)
    (environment : Option String := none) : IO A := do
  let apiKey ← show IO String from do 
    match apiKey with
      | some apiKey => return apiKey
      | none => 
        let some apiKey ← IO.getEnv "PINECONE_API_KEY" | 
          throw <| .userError "Pinecone API key not found in environment."
        return apiKey
  let project ← show IO String from do 
    match project with
      | some projectId => return projectId
      | none => 
        let some projectId ← IO.getEnv "PINECONE_PROJECT_ID" | 
          throw <| .userError "Pinecone project ID not found in environment."
        return projectId
  let environment ← show IO String from do 
    match environment with
      | some env => return env
      | none => 
        let some projectId ← IO.getEnv "PINECONE_ENVIRONMENT" | 
          throw <| .userError "Pinecone environment not found in environment."
        return projectId
  let index ← show IO String from do
    match index with
      | some index => return index
      | none => 
        let some index ← IO.getEnv "PINECONE_INDEX" | 
          throw <| .userError "Pinecone index not found in environment."
        return index
  ReaderT.run m { apiKey := apiKey, project := project, index := index, environment := environment }

def run {A : Type _} (m : PineconeM A) : IO A := runWith m

end PineconeM