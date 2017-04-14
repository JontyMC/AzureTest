#r "Newtonsoft.Json"

using System;
using System.Net;
using Dapper;
using System.Data.SqlClient;
using System.Configuration;
using Newtonsoft.Json;

static TraceWriter log;

public static void Run(Stream sessionBlob, string name, TraceWriter logger)
{
    log = logger;
    log.Info($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {sessionBlob.Length} Bytes");
    var session = ParseSessionFromBlob(sessionBlob);
    SaveSession(session);
}

public static Session ParseSessionFromBlob(Stream sessionBlob)
{
    var serializer = new JsonSerializer();
    using (var sr = new StreamReader(sessionBlob))
    using (var jsonTextReader = new JsonTextReader(sr))
    {
        return serializer.Deserialize<Session>(jsonTextReader);
    }
}

public static void SaveSession(Session session)
{
    var connStr  = ConfigurationManager.ConnectionStrings["osso"].ConnectionString;
    using(var connection = new SqlConnection(connStr))
    {
        connection.Open();

        using(var trans = connection.BeginTransaction())
        {
            connection.Execute(
                "INSERT INTO Session (Id, UserId, Start, [End]) " +
                "VALUES (@Id, @UserId, @Start, @End)", session, trans);
            connection.Execute(
                "INSERT INTO Step (Id, SessionId, ParentId, Name, Start, [End]) " +
                "VALUES (@Id, @SessionId, @ParentId, @Name, @Start, @End)", session.Steps, trans);
            trans.Commit();
        }
    }
}

public class Session
{
    public Guid Id { get; set; }
    public string IpAddress { get; set; }
    public string OssoVersionNumber { get; set; }
    public string OssoProduct { get; set; }
    public Guid UserId { get; set; }

    [JsonProperty(PropertyName = "sessionStartTime")]
    public DateTime Start { get; set; }

    [JsonProperty(PropertyName = "sessionEndTime")]
    public DateTime End { get; set; }

    [JsonProperty(PropertyName = "procedureSteps")]
    public Step[] Steps { get; set; }
}

public class Step
{
    public Guid Id { get; set; }
    public Guid SessionId { get; set; }

    [JsonProperty(PropertyName = "parentProcedureStepId")]
    public Guid? ParentId { get; set; }

    [JsonProperty(PropertyName = "stepName")]
    public string Name { get; set; }

    [JsonProperty(PropertyName = "unityStartTime")]
    public decimal Start { get; set; }

    [JsonProperty(PropertyName = "unityCompleteTime")]
    public decimal End { get; set; }
}