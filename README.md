# Arrow-Parquet-Go

This is a pure-go implementation for reading and writing parquet files into Arrow Record batches. 

## Install
``` go get github.com/mindhash/arrow-parquet-go ```

## Usage

For Reading a file: 

``` 
    readerFunc := func(offset, length int64) (io.ReadCloser, error) {
        file, err := os.Open(parquet_file_name)
        if err != nil {
            panic(err)
        }   
        offset := 0 
        file.Seek(offset, os.SEEK_SET)
        return file, nil
	}

    reader, err := NewReader(readerFunc)
    for {
		record, err := reader.Read()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}

			break
		}
        fmt.Println("batch record count:", record.NumRows())
        record.Release()
    }
    reader.Close()

```


#### Attribution
This is modified version of [this repo](https://github.com/minio/parquet-go) with support Arrow batches.

