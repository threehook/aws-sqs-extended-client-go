package aws_sqs_extended_client_go

import "bytes"

func getStringSizeInBytes(str string) int32 {
	var countBuf bytes.Buffer
	countBuf.WriteString(str)
	return int32(countBuf.Len())
}
