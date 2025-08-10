Imports MySql.Data.MySqlClient
Imports System.Runtime.CompilerServices

Public Module MySqlExtensions
    <Extension()>
    Public Function Query(Of T)(conn As MySqlConnection, sql As String, Optional param As Object = Nothing) As IEnumerable(Of T)
        Using cmd As New MySqlCommand(sql, conn)
            If param IsNot Nothing Then
                AddParameters(cmd, param)
            End If

            Using reader = cmd.ExecuteReader()
                Dim results As New List(Of T)
                Dim props = GetType(T).GetProperties()

                While reader.Read()
                    Dim obj = Activator.CreateInstance(Of T)()
                    For Each prop In props
                        Try
                            If Not reader.IsDBNull(reader.GetOrdinal(prop.Name)) Then
                                Dim value = reader(prop.Name)
                                prop.SetValue(obj, If(value Is DBNull.Value, Nothing, value))
                            End If
                        Catch ex As Exception
                            ' Column not found, skip
                        End Try
                    Next
                    results.Add(obj)
                End While

                Return results
            End Using
        End Using
    End Function

    <Extension()>
    Public Function QueryFirstOrDefault(Of T)(conn As MySqlConnection, sql As String, Optional param As Object = Nothing) As T
        Return Query(Of T)(conn, sql, param).FirstOrDefault()
    End Function

    <Extension()>
    Public Function Execute(conn As MySqlConnection, sql As String, Optional param As Object = Nothing) As Integer
        Using cmd As New MySqlCommand(sql, conn)
            If param IsNot Nothing Then
                AddParameters(cmd, param)
            End If
            Return cmd.ExecuteNonQuery()
        End Using
    End Function

    ' Async versions
    <Extension()>
    Public Async Function QueryAsync(Of T)(conn As MySqlConnection, sql As String, Optional param As Object = Nothing) As Task(Of IEnumerable(Of T))
        Using cmd As New MySqlCommand(sql, conn)
            If param IsNot Nothing Then
                AddParameters(cmd, param)
            End If

            Using reader = Await cmd.ExecuteReaderAsync()
                Dim results As New List(Of T)
                Dim props = GetType(T).GetProperties()

                While Await reader.ReadAsync()
                    Dim obj = Activator.CreateInstance(Of T)()
                    For Each prop In props
                        Try
                            If Not reader.IsDBNull(reader.GetOrdinal(prop.Name)) Then
                                Dim value = reader(prop.Name)
                                prop.SetValue(obj, If(value Is DBNull.Value, Nothing, value))
                            End If
                        Catch ex As Exception
                            ' Column not found, skip
                        End Try
                    Next
                    results.Add(obj)
                End While

                Return results
            End Using
        End Using
    End Function

    <Extension()>
    Public Async Function QueryFirstOrDefaultAsync(Of T)(conn As MySqlConnection, sql As String, Optional param As Object = Nothing) As Task(Of T)
        Dim results = Await QueryAsync(Of T)(conn, sql, param)
        Return results.FirstOrDefault()
    End Function

    <Extension()>
    Public Async Function ExecuteAsync(conn As MySqlConnection, sql As String, Optional param As Object = Nothing) As Task(Of Integer)
        Using cmd As New MySqlCommand(sql, conn)
            If param IsNot Nothing Then
                AddParameters(cmd, param)
            End If
            Return Await cmd.ExecuteNonQueryAsync()
        End Using
    End Function

    Private Sub AddParameters(cmd As MySqlCommand, param As Object)
        For Each prop In param.GetType().GetProperties()
            Dim value = prop.GetValue(param)
            cmd.Parameters.AddWithValue($"@{prop.Name}", If(value, DBNull.Value))
        Next
    End Sub
End Module