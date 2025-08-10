Imports MySql.Data.MySqlClient
Imports System.Collections.Generic
Imports System.Threading.Tasks
Imports System.Runtime.CompilerServices

Public Class GenericRepository(Of T As Class)
    Private ReadOnly _connectionString As String

    Public Sub New(connectionString As String)
        _connectionString = connectionString
    End Sub

    ' SYNC METHODS

    Public Function GetAll() As List(Of T)
        Using conn As New MySqlConnection(_connectionString)
            conn.Open()
            Dim tableName = GetTableName()
            Return conn.Query(Of T)($"SELECT * FROM {tableName}").ToList()
        End Using
    End Function

    Public Function GetById(id As Object) As T
        Using conn As New MySqlConnection(_connectionString)
            conn.Open()
            Dim tableName = GetTableName()
            Dim idColumn = GetIdColumnName()
            Return conn.QueryFirstOrDefault(Of T)(
                $"SELECT * FROM {tableName} WHERE {idColumn} = @id",
                New With {.id = id})
        End Using
    End Function

    Public Sub Insert(entity As T)
        Using conn As New MySqlConnection(_connectionString)
            conn.Open()
            Dim tableName = GetTableName()
            Dim properties = GetWritableProperties()
            Dim columns = String.Join(", ", properties.Select(Function(p) p.Name))
            Dim values = String.Join(", ", properties.Select(Function(p) $"@{p.Name}"))

            Dim sql = $"INSERT INTO {tableName} ({columns}) VALUES ({values})"
            conn.Execute(sql, entity)
        End Using
    End Sub

    Public Sub Update(entity As T)
        Using conn As New MySqlConnection(_connectionString)
            conn.Open()
            Dim tableName = GetTableName()
            Dim idColumn = GetIdColumnName()
            Dim properties = GetWritableProperties()
            Dim setClause = String.Join(", ",
                properties.Where(Function(p) p.Name <> idColumn).
                Select(Function(p) $"{p.Name} = @{p.Name}"))

            Dim sql = $"UPDATE {tableName} SET {setClause} WHERE {idColumn} = @{idColumn}"
            conn.Execute(sql, entity)
        End Using
    End Sub

    Public Sub Delete(id As Object)
        Using conn As New MySqlConnection(_connectionString)
            conn.Open()
            Dim tableName = GetTableName()
            Dim idColumn = GetIdColumnName()
            conn.Execute(
                $"DELETE FROM {tableName} WHERE {idColumn} = @id",
                New With {.id = id})
        End Using
    End Sub

    ' ASYNC METHODS

    Public Async Function GetAllAsync() As Task(Of List(Of T))
        Using conn As New MySqlConnection(_connectionString)
            Await conn.OpenAsync()
            Dim tableName = GetTableName()
            Return (Await conn.QueryAsync(Of T)($"SELECT * FROM {tableName}")).ToList()
        End Using
    End Function

    Public Async Function GetByIdAsync(id As Object) As Task(Of T)
        Using conn As New MySqlConnection(_connectionString)
            Await conn.OpenAsync()
            Dim tableName = GetTableName()
            Dim idColumn = GetIdColumnName()
            Return Await conn.QueryFirstOrDefaultAsync(Of T)(
                $"SELECT * FROM {tableName} WHERE {idColumn} = @id",
                New With {.id = id})
        End Using
    End Function

    Public Async Function InsertAsync(entity As T) As Task
        Using conn As New MySqlConnection(_connectionString)
            Await conn.OpenAsync()
            Dim tableName = GetTableName()
            Dim properties = GetWritableProperties()
            Dim columns = String.Join(", ", properties.Select(Function(p) p.Name))
            Dim values = String.Join(", ", properties.Select(Function(p) $"@{p.Name}"))

            Dim sql = $"INSERT INTO {tableName} ({columns}) VALUES ({values})"
            Await conn.ExecuteAsync(sql, entity)
        End Using
    End Function

    Public Async Function UpdateAsync(entity As T) As Task
        Using conn As New MySqlConnection(_connectionString)
            Await conn.OpenAsync()
            Dim tableName = GetTableName()
            Dim idColumn = GetIdColumnName()
            Dim properties = GetWritableProperties()
            Dim setClause = String.Join(", ",
                properties.Where(Function(p) p.Name <> idColumn).
                Select(Function(p) $"{p.Name} = @{p.Name}"))

            Dim sql = $"UPDATE {tableName} SET {setClause} WHERE {idColumn} = @{idColumn}"
            Await conn.ExecuteAsync(sql, entity)
        End Using
    End Function

    Public Async Function DeleteAsync(id As Object) As Task
        Using conn As New MySqlConnection(_connectionString)
            Await conn.OpenAsync()
            Dim tableName = GetTableName()
            Dim idColumn = GetIdColumnName()
            Await conn.ExecuteAsync(
                $"DELETE FROM {tableName} WHERE {idColumn} = @id",
                New With {.id = id})
        End Using
    End Function

    ' HELPER METHODS (same as before)
    Private Function GetTableName() As String
        Dim type = GetType(T)
        If type.Name.EndsWith("s") Then
            Return type.Name.Substring(0, type.Name.Length - 1)
        End If
        Return type.Name
    End Function

    Private Function GetIdColumnName() As String
        Dim type = GetType(T)
        Dim defaultId = $"{type.Name}Id"

        If GetWritableProperties().Any(Function(p) p.Name.Equals(defaultId, StringComparison.OrdinalIgnoreCase)) Then
            Return defaultId
        End If

        Return "Id"
    End Function

    Private Function GetWritableProperties() As IEnumerable(Of System.Reflection.PropertyInfo)
        Return GetType(T).GetProperties().
            Where(Function(p) p.CanWrite AndAlso
                  Not p.Name.EndsWith("List", StringComparison.OrdinalIgnoreCase))
    End Function
End Class


