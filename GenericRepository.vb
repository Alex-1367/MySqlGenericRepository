Imports MySql.Data.MySqlClient
Imports System.Collections.Generic
Imports System.Threading.Tasks
Imports System.Runtime.CompilerServices
Imports System.Reflection

Public Class GenericRepository(Of T As Class)
    Implements IGenericRepository(Of T)

    Private ReadOnly _connectionString As String
    Private ReadOnly _keyColumnName As String
    Private ReadOnly _keyType As Type
    Private ReadOnly _autoManagedFields As HashSet(Of String)

    Public Sub New(connectionString As String,
                  Optional keyType As Type = Nothing,
                  Optional keyColumnName As String = "Id",
                  Optional SetAutomaticallyByDb As IEnumerable(Of String) = Nothing)
        _connectionString = connectionString
        _keyColumnName = keyColumnName

        ' Initialize auto-managed fields
        _autoManagedFields = If(SetAutomaticallyByDb IsNot Nothing,
                              New HashSet(Of String)(SetAutomaticallyByDb, StringComparer.OrdinalIgnoreCase),
                              New HashSet(Of String)(StringComparer.OrdinalIgnoreCase))

        ' Determine key type automatically if not specified
        If keyType Is Nothing Then
            Dim idProperty = GetType(T).GetProperties().
                FirstOrDefault(Function(p) p.Name.Equals(keyColumnName, StringComparison.OrdinalIgnoreCase))

            If idProperty IsNot Nothing Then
                _keyType = idProperty.PropertyType
            Else
                ' Default to Integer if not found
                _keyType = GetType(Integer)
            End If
        Else
            _keyType = keyType
        End If
    End Sub

    ' SYNC METHODS
    Public Function GetAll() As List(Of T) Implements IGenericRepository(Of T).GetAll
        Using conn As New MySqlConnection(_connectionString)
            conn.Open()
            Dim tableName = GetTableName()
            Return conn.Query(Of T)($"SELECT * FROM {tableName}").ToList()
        End Using
    End Function

    Public Function GetById(id As Object) As T Implements IGenericRepository(Of T).GetById
        Dim typedId = Convert.ChangeType(id, _keyType)

        Using conn As New MySqlConnection(_connectionString)
            conn.Open()
            Dim tableName = GetTableName()
            Return conn.QueryFirstOrDefault(Of T)(
                $"SELECT * FROM {tableName} WHERE {_keyColumnName} = @id",
                New With {.id = typedId})
        End Using
    End Function

    Public Sub Insert(entity As T) Implements IGenericRepository(Of T).Insert
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

    Public Sub Update(entity As T) Implements IGenericRepository(Of T).Update
        Using conn As New MySqlConnection(_connectionString)
            conn.Open()
            Dim tableName = GetTableName()
            Dim properties = GetWritableProperties()
            Dim setClause = String.Join(", ",
                properties.Where(Function(p) p.Name <> _keyColumnName).
                Select(Function(p) $"{p.Name} = @{p.Name}"))

            Dim sql = $"UPDATE {tableName} SET {setClause} WHERE {_keyColumnName} = @{_keyColumnName}"
            conn.Execute(sql, entity)
        End Using
    End Sub

    Public Sub Delete(id As Object) Implements IGenericRepository(Of T).Delete
        Dim typedId = Convert.ChangeType(id, _keyType)

        Using conn As New MySqlConnection(_connectionString)
            conn.Open()
            Dim tableName = GetTableName()
            conn.Execute(
                $"DELETE FROM {tableName} WHERE {_keyColumnName} = @id",
                New With {.id = typedId})
        End Using
    End Sub

    ' ASYNC METHODS
    Public Async Function GetAllAsync() As Task(Of List(Of T)) Implements IGenericRepository(Of T).GetAllAsync
        Using conn As New MySqlConnection(_connectionString)
            Await conn.OpenAsync()
            Dim tableName = GetTableName()
            Return (Await conn.QueryAsync(Of T)($"SELECT * FROM {tableName}")).ToList()
        End Using
    End Function

    Public Async Function GetByIdAsync(id As Object) As Task(Of T) Implements IGenericRepository(Of T).GetByIdAsync
        Dim typedId = Convert.ChangeType(id, _keyType)

        Using conn As New MySqlConnection(_connectionString)
            Await conn.OpenAsync()
            Dim tableName = GetTableName()
            Return Await conn.QueryFirstOrDefaultAsync(Of T)(
                $"SELECT * FROM {tableName} WHERE {_keyColumnName} = @id",
                New With {.id = typedId})
        End Using
    End Function

    Public Async Function InsertAsync(entity As T) As Task Implements IGenericRepository(Of T).InsertAsync
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

    Public Async Function InsertAndReturnIdAsync(entity As T) As Task(Of Integer) Implements IGenericRepository(Of T).InsertAndReturnIdAsync
        Using conn As New MySqlConnection(_connectionString)
            Await conn.OpenAsync()
            Dim tableName = GetTableName()

            ' Get properties excluding key column and auto-managed fields
            Dim properties = GetType(T).GetProperties().
            Where(Function(p) p.CanWrite AndAlso
                  Not p.Name.Equals(_keyColumnName, StringComparison.OrdinalIgnoreCase) AndAlso
                  Not _autoManagedFields.Contains(p.Name) AndAlso
                  Not p.Name.EndsWith("List", StringComparison.OrdinalIgnoreCase))

            Dim columns = String.Join(", ", properties.Select(Function(p) p.Name))
            Dim values = String.Join(", ", properties.Select(Function(p) $"@{p.Name}"))

            Dim sql = $"INSERT INTO {tableName} ({columns}) VALUES ({values}); SELECT LAST_INSERT_ID();"
            Return Await conn.ExecuteScalarAsync(Of Integer)(sql, entity)
        End Using
    End Function

    Public Async Function UpdateAsync(entity As T) As Task Implements IGenericRepository(Of T).UpdateAsync
        Using conn As New MySqlConnection(_connectionString)
            Await conn.OpenAsync()
            Dim tableName = GetTableName()
            Dim properties = GetWritableProperties()
            Dim setClause = String.Join(", ",
                properties.Where(Function(p) p.Name <> _keyColumnName).
                Select(Function(p) $"{p.Name} = @{p.Name}"))

            Dim sql = $"UPDATE {tableName} SET {setClause} WHERE {_keyColumnName} = @{_keyColumnName}"
            Await conn.ExecuteAsync(sql, entity)
        End Using
    End Function

    Public Async Function DeleteAsync(id As Object) As Task Implements IGenericRepository(Of T).DeleteAsync
        Dim typedId = Convert.ChangeType(id, _keyType)

        Using conn As New MySqlConnection(_connectionString)
            Await conn.OpenAsync()
            Dim tableName = GetTableName()
            Await conn.ExecuteAsync(
                $"DELETE FROM {tableName} WHERE {_keyColumnName} = @id",
                New With {.id = typedId})
        End Using
    End Function

    Public Async Function GetCountAsync() As Task(Of Integer) Implements IGenericRepository(Of T).GetCountAsync
        Using conn As New MySqlConnection(_connectionString)
            Await conn.OpenAsync()
            Dim tableName = GetTableName()
            Return Await conn.ExecuteScalarAsync(Of Integer)($"SELECT COUNT(*) FROM {tableName}")
        End Using
    End Function

    Public Async Function GetAllWithProgressAsync(progressCallback As Action(Of Integer, Integer)) As Task(Of List(Of T)) Implements IGenericRepository(Of T).GetAllWithProgressAsync
        Using conn As New MySqlConnection(_connectionString)
            Await conn.OpenAsync()
            Return Await conn.QueryWithProgressAsync(Of T)(
            $"SELECT * FROM {GetTableName()}",
            progressCallback)
        End Using
    End Function

    Private Function GetTableName() As String
        Dim type = GetType(T)
        If type.Name.EndsWith("s") Then
            Return type.Name.Substring(0, type.Name.Length - 1)
        End If
        Return type.Name
    End Function

    Private Function GetWritableProperties() As IEnumerable(Of PropertyInfo)
        Return GetType(T).GetProperties().
            Where(Function(p) p.CanWrite AndAlso
                  Not p.Name.EndsWith("List", StringComparison.OrdinalIgnoreCase) AndAlso
                  Not _autoManagedFields.Contains(p.Name))
    End Function

End Class