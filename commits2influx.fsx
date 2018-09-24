#r "./packages/System.Net.Http/lib/net46/System.Net.Http.dll"
#r "./packages/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"

open System;
open System.Globalization;
open System.Net.Http;
open System.Text.RegularExpressions;

[<CLIMutableAttribute>]
type GitLabCommit =
    { id : string;
      committed_date : DateTime; }

[<CLIMutableAttribute>]
type GitLabCommitStats =
    { additions: int;
      deletions: int; }

[<CLIMutableAttribute>]
type GitLabCommitWithStats =
    { committer_email : string;
      committed_date : DateTime;
      message : string;
      id : string;
      stats : GitLabCommitStats }

[<CLIMutableAttribute>]
type GitLabProject =
    { id : string;
      name : string; }

type InfluxPoint =
    { Name : string;
      TagSet : Map<string, string>;
      FieldSet : Map<string, int>;
      Timestamp : int64 }

let deserialize<'T> content = Newtonsoft.Json.JsonConvert.DeserializeObject<'T>(content)
let toUtc datetime = TimeZoneInfo.ConvertTimeToUtc(datetime)
let toStr m = m |> Map.toList |> List.map (fun (p, k) -> sprintf "%s=%O" p k) |> String.concat ","

let getAsync (url : string) = async {
    let httpClient = new HttpClient()
    let! response = httpClient.GetAsync(url) |> Async.AwaitTask
    response.EnsureSuccessStatusCode() |> ignore
    let! content = response.Content.ReadAsStringAsync() |> Async.AwaitTask
    return content }

let getGroupProjectsAsync (baseUrl : string) (groupId : string) (privateToken : string) = async {
    let rec getProjectByPage (page : int) (perpage : int) = async {
        let groupUrl =
            sprintf "%s/api/v4/groups/%s/projects?private_token=%s&page=%d&per_page=%d"
                baseUrl
                groupId
                privateToken
                page
                perpage

        let! content = getAsync groupUrl
        let projects = deserialize<GitLabProject list> content
        if projects.IsEmpty
        then return projects

        else
            let! next = getProjectByPage (page + 1) perpage
            return projects |> List.append next }

    let! projects = getProjectByPage 1 20
    return projects }

let getCommitStatsAsync baseUrl projectId privateToken commitId = async {
    let commitUrl =
        sprintf "%s/api/v4/projects/%s/repository/commits/%s?private_token=%s"
            baseUrl
            projectId
            commitId
            privateToken
    let! content = getAsync commitUrl
    return deserialize<GitLabCommitWithStats> content }

let rec getProjectCommitsAsync (baseUrl : string) (projectId : string) (privateToken : string) (until : DateTime) = async {
    let getProjectCommitsSince (until : DateTime) = async {
        let projectUrl =
            sprintf "%s/api/v4/projects/%s/repository/commits?private_token=%s&until=%s"
                baseUrl
                projectId
                privateToken
                (until.ToString("o", CultureInfo.InvariantCulture))
        let! content = getAsync projectUrl
        let commits = deserialize<GitLabCommit list> content
        let least =
            if commits.IsEmpty then None else Some (commits |> List.last).committed_date
        let! commitStats =
            commits
            |> List.map (fun c -> getCommitStatsAsync baseUrl projectId privateToken c.id)
            |> Async.Parallel
        return (commitStats, least) }

    let! commits, least = getProjectCommitsSince until
    match least with
    | Some(x) ->
        let! next = getProjectCommitsAsync baseUrl projectId privateToken (toUtc(x.AddMilliseconds(-1.0)))
        return (commits |> Array.append next)
    | None -> return commits }


let getIssue (prefix : string) text =
    let reg = Regex(prefix + "-\d+")
    let m = reg.Match(text)
    if m.Success then m.Value else "none"

let convertToInflux (prefix : string) (commits : GitLabCommitWithStats[] * string) =
    let entries, name = commits
    entries
    |> Array.map (fun p ->
                    { Name = "commits";
                      TagSet =
                        [ "committer", p.committer_email;
                          "repository", name;
                          "issue", p.message |> getIssue prefix]
                        |> Map.ofList;
                      FieldSet =
                        [ "additions", p.stats.additions;
                          "deletions", p.stats.deletions; ] |> Map.ofList;
                      Timestamp = int64(p.committed_date.Subtract(new DateTime(1970, 1, 1)).TotalSeconds) })
    |> Seq.ofArray

let printOut (points : InfluxPoint seq) =
    points
    |> Seq.map (fun p -> String.Format("{0},{1} {2} {3}", p.Name, (p.TagSet |> toStr), (p.FieldSet |> toStr), p.Timestamp))
    |> Seq.iter (printfn "%s")


let getGroupCommits baseUrl groupId privateToken = async {
    let! projects = getGroupProjectsAsync baseUrl groupId privateToken
    let! commits =
        projects
        |> List.map (fun p -> async {
                        let! commits = getProjectCommitsAsync baseUrl p.id privateToken (DateTime.UtcNow)
                        return (commits, p.name) })
        |> Async.Parallel
    return commits }


let arguments = System.Environment.GetCommandLineArgs()

let result =
    getGroupCommits arguments.[2] arguments.[3] arguments.[4]
    |> Async.RunSynchronously

result
|> Seq.ofArray
|> Seq.collect (convertToInflux arguments.[5])
|> printOut
