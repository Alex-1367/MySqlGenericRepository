Public Interface IGenericRepository(Of T As Class)
    Function GetAll() As List(Of T)
    Function GetById(id As Object) As T
    Sub Insert(entity As T)
    Sub Update(entity As T)
    Sub Delete(id As Object)

    Function GetAllAsync() As Task(Of List(Of T))
    Function GetByIdAsync(id As Object) As Task(Of T)
    Function InsertAsync(entity As T) As Task
    Function InsertAndReturnIdAsync(entity As T) As Task(Of Integer)
    Function UpdateAsync(entity As T) As Task
    Function DeleteAsync(id As Object) As Task
    Function GetCountAsync() As Task(Of Integer)
    Function GetAllWithProgressAsync(progressCallback As Action(Of Integer, Integer)) As Task(Of List(Of T))
End Interface
