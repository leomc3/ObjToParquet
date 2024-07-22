using Newtonsoft.Json;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using System.Collections;
using System.Reflection;

namespace ObjToParquet
{
    public class Program
    {
        static Dictionary<string, List<object>> GetColumns<T>(List<T> dataList)
        {
            var columns = new Dictionary<string, List<object>>();
            var properties = typeof(T).GetProperties();

            foreach (var property in properties)
            {
                var jsonProperty = property.GetCustomAttribute<JsonPropertyAttribute>()?.PropertyName ?? property.Name;
                var values = dataList.Select(f => property.GetValue(f)).ToList();
                columns[jsonProperty] = values;
            }

            return columns;
        }
        public static Array DynamicCastListToArray3(IEnumerable<object> list, Type targetType, bool isNullable)
        {
            if (isNullable)
                targetType = targetType switch
                {
                    Type t when t == typeof(DateTime) => typeof(DateTime?),
                    Type t when t == typeof(int) => typeof(int?),
                    Type t when t == typeof(double) => typeof(double?),
                    Type t when t == typeof(decimal) => typeof(decimal?),
                    _ => throw new NotSupportedException($"Tipo de dado {targetType} não suportado.")
                };

            // Handle nullable types
            Type underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

            // Create a generic List of the target type
            var listType = typeof(List<>).MakeGenericType(targetType);
            var castedList = (IList)Activator.CreateInstance(listType);

            // Use reflection to get the Add method of the generic List
            var addMethod = listType.GetMethod("Add");

            // Cast each item in the list and add it to the castedList
            foreach (var item in list)
            {
                object castedItem = item == null ? null : Convert.ChangeType(item, underlyingType);
                addMethod.Invoke(castedList, new object[] { castedItem });
            }

            // Convert the list to an array
            var toArrayMethod = typeof(Enumerable).GetMethod("ToArray").MakeGenericMethod(targetType);
            return (Array)toArrayMethod.Invoke(null, new object[] { castedList });
        }
        public static List<DataField> ConvertToColum<T>(List<T> file)
        {
            List<DataField> ret = new List<DataField>();
            Type type = file.FirstOrDefault().GetType();
            PropertyInfo[] properties = type.GetProperties();

            foreach (PropertyInfo property in properties)
            {
                var jsonPropertyAttribute = property.GetCustomAttribute<JsonPropertyAttribute>();
                string columnName = jsonPropertyAttribute != null ? jsonPropertyAttribute.PropertyName : property.Name;
                int teste = 1;
                if (columnName.Equals("CODENRMQN"))
                    teste = 2;
                Type _type = property.PropertyType;
                bool flagNull = false;
                if (_type.IsGenericType && _type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    _type = Nullable.GetUnderlyingType(_type);
                    flagNull = true;
                }

                //DataField dataField = 
                ret.Add(_type switch
                {
                    Type t when t == typeof(string) => new DataField<string>(columnName, flagNull),
                    Type t when t == typeof(int) => new DataField<int>(columnName, flagNull),
                    Type t when t == typeof(Int64) => new DataField<Int64>(columnName, flagNull),
                    Type t when t == typeof(decimal) => new DataField<decimal>(columnName, flagNull),
                    Type t when t == typeof(double) => new DataField<double>(columnName, flagNull),
                    Type t when t == typeof(DateTime) => new DataField<DateTime>(columnName, flagNull),
                    Type t when t == typeof(DateTime?) => new DataField<DateTime?>(columnName, flagNull),
                    _ => throw new NotSupportedException($"Tipo de dado {_type} não suportado.")
                });
            }
            return ret;
        }

        public static bool EscreveArquivoParquet<T>(List<T> file, string dirNameParque)
        {
            var retorno = false;

            var arrColum = ConvertToColum<T>(file).OrderBy(x => x.Name).ToArray();

            //var schema = new ParquetSchema(arrColum[0..9]); 
            var schema = new ParquetSchema(arrColum);
            var arrDataColum = new List<DataColumn>();
            var dicCol = GetColumns(file);

            foreach (var item in schema.DataFields)
            {
                var data = dicCol[item.Name];
                var castedList = DynamicCastListToArray3(data, item.ClrType, item.IsNullable);
                arrDataColum.Add(new DataColumn(
                item,
                castedList));
            }

            if (!Directory.Exists(Path.GetDirectoryName(dirNameParque)))
                Directory.CreateDirectory(Path.GetDirectoryName(dirNameParque));


            if (File.Exists(dirNameParque))
                File.Delete(dirNameParque);

            using (FileStream fs = File.OpenWrite(dirNameParque))
            {
                using (ParquetWriter writer = ParquetWriter.CreateAsync(schema, fs).Result)
                {
                    using (ParquetRowGroupWriter groupWriter = writer.CreateRowGroup())
                    {
                        foreach (var item in arrDataColum)
                            groupWriter.WriteColumnAsync(item);
                    }
                }
            }

            return true;
        }
        public class Obj
        {
            public Obj(int sequence, string nomeFile, DateTime data)
            {
                this.sequence = sequence;
                this.nomeFile = nomeFile;
                this.data = data;
            }

            [JsonProperty("NUMSEQ")]
            public int sequence { get; set; }
            [JsonProperty("NOME")]
            public string nomeFile { get; set; }
            [JsonProperty("DATA")]
            public DateTime data { get; set; }
        }

        public static void Main(string[] args)
        {
            try
            {
                string fileNameParque = "arquivoName.parquet";
                
                bool sucesso = EscreveArquivoParquet(new List<Obj>() { new Obj(1, "a", DateTime.Now) }, Path.Combine(Directory.GetCurrentDirectory(), fileNameParque));
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

    }
}