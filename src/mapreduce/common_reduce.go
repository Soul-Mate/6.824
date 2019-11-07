package mapreduce

import (
	"encoding/json"
	"io"
	"os"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string,       // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string,       // write the output here
	nMap int,             // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// 你需要编写此函数
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// 你需要从每个 map 任务中读取一个中间文件;
	// reduceName(jobName, m, reduceTaskNumber) 可以生成 map 任务 m 的文件名.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// doMap() 函数对中间文件 key/value 进行了编码, 因此你需要进行解码.
	//  如果使用json, 你可以创建解码器调用 .Decode(&kv) 重复解码和读取,
	//  直到返回错误
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// 你可能会发现 golang sort 包文档中的第一个示例很有用
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	var kvs []KeyValue

	// 读取 map 任务生成的中间文件到内存
	for i := 0; i < nMap; i++ {
		r, err := os.Open(reduceName(jobName, i, reduceTaskNumber))
		if err != nil {
			panic(err)
		}

		// map 中使用了json 对 KeyValue 进行编码,
		// 因此这里使用 json 进行解码
		dec := json.NewDecoder(r)

		var kv KeyValue

		for {
			if err = dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}

				panic(err)
			}

			kvs = append(kvs, kv)
		}
	}


	// 原文中需要对key进行排序来聚合相同的key
	// 这里感觉使用map结构会更方便
	m := make(map[string][]string)

	for _, kv := range kvs {
		m[kv.Key] = append(m[kv.Key], kv.Value)
	}


	w, err := os.Create(outFile)

	if err != nil {
		panic(err)
	}

	enc := json.NewEncoder(w)

	for k, v := range m {
		if err = enc.Encode(KeyValue{k, reduceF(k, v)}); err != nil && err != io.EOF {
			panic(err)
		}
	}
}
