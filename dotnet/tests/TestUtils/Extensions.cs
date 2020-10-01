using Streamiz.Kafka.Net.Mock;
using System.Collections.Generic;

namespace TestUtils
{
    /// <summary>
    /// TODO : Need to add this extensions method to Streamiz package.
    /// </summary>
    public static class Extensions
    {
        public static Dictionary<K, V> ReadKeyValuesToMap<K, V>(this TestOutputTopic<K, V> outputTopic)
        {
            Dictionary<K, V> map = new Dictionary<K, V>();

            var result = outputTopic.ReadKeyValueList();
            foreach (var r in result)
            {
                if (map.ContainsKey(r.Message.Key))
                {
                    map[r.Message.Key] = r.Message.Value;
                }
                else
                {
                    map.Add(r.Message.Key, r.Message.Value);
                }
            }

            return map;
        }
    }

}
