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

        public static Array DynamicCastListToArray(IEnumerable<object> list, Type targetType, bool isNullable)
        {
            if (isNullable)
            {
                targetType = targetType switch
                {
                    Type t when t == typeof(DateTime) => typeof(DateTime?),
                    Type t when t == typeof(int) => typeof(int?),
                    Type t when t == typeof(double) => typeof(double?),
                    Type t when t == typeof(decimal) => typeof(decimal?),
                    _ => throw new NotSupportedException($"Tipo de dado {targetType} não suportado.")
                };
            }

            Type underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;
            var castedList = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(targetType));
            var addMethod = castedList.GetType().GetMethod("Add");

            foreach (var item in list)
            {
                addMethod.Invoke(castedList, new object[] { item == null ? null : Convert.ChangeType(item, underlyingType) });
            }

            return castedList.GetType().GetMethod("ToArray").Invoke(castedList, null) as Array;
        }

        public static List<DataField> ConvertToColumns<T>(List<T> file)
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
            var schema = new ParquetSchema(ConvertToColumns(file).OrderBy(x => x.Name).ToArray());
            var dicCol = GetColumns(file);

            var arrDataColum = schema.DataFields.Select(item =>
            {
                var data = dicCol[item.Name];
                var castedList = DynamicCastListToArray(data, item.ClrType, item.IsNullable);
                return new DataColumn(item, castedList);
            }).ToList();

            Directory.CreateDirectory(Path.GetDirectoryName(dirNameParque) ?? string.Empty);
            if (File.Exists(dirNameParque))
                File.Delete(dirNameParque);

            using (var fs = File.OpenWrite(dirNameParque))
            {
                using (var writer = ParquetWriter.CreateAsync(schema, fs).Result)
                {
                    using (var groupWriter = writer.CreateRowGroup())
                    {
                        foreach (var column in arrDataColum)
                        {
                            groupWriter.WriteColumnAsync(column).Wait();
                        }
                    }
                }
            }

            return true;
        }

        public class Obj
        {
            public Obj(int sequence, string nomeFile, DateTime data)
            {
                Sequence = sequence;
                NomeFile = nomeFile;
                Data = data;
            }

            [JsonProperty("NUMSEQ")]
            public int Sequence { get; set; }
            [JsonProperty("NOME")]
            public string NomeFile { get; set; }
            [JsonProperty("DATA")]
            public DateTime Data { get; set; }
        }

        public static void Main(string[] args)
        {
            try
            {
                string fileNameParque = "arquivoName.parquet";
                var filePath = Path.Combine(Directory.GetCurrentDirectory(), fileNameParque);
                EscreveArquivoParquet(new List<Obj> { new Obj(1, "a", DateTime.Now) }, filePath);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error: {ex.Message}");
            }
        }
    }
}
