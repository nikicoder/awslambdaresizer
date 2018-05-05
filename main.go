package main

import (
	"bufio"
	"encoding/base64"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/disintegration/imaging"
	"gopkg.in/h2non/filetype.v1"
)

const tmp = "/tmp/"

// S3 Session to use
var sess = session.Must(session.NewSession())

// Create an uploader with session and default option
var uploader = s3manager.NewUploader(sess)

// Create a downloader with session and default option
var downloader = s3manager.NewDownloader(sess)

type Request struct {
	Src_key      string `json:"src_key"`
	Src_bucket   string `json:"src_bucket"`
	Dst_bucket   string `json:"dst_bucket"`
	Root_folder  string `json:"root_folder"`
	Preset_name  string `json:"preset_name"`
	Rewrite_part string `json:"rewrite_part"`
	Img_width    int    `json:"width"`
}

type Response struct {
	Status      bool   `json:"status"`
	Key         string `json:"key"`
	ContentType string `json:"content_type"`
	Data        string `json:"data"`
	Error_msg   string `json:"error"`
}

type Resize_result struct {
	Key         string
	ContentType string
	Data        string
}

func main() {
	lambda.Start(handle)
}

func handle(request Request) (Response, error) {

	var new_key Resize_result
	source_file_path, err := getSource(request.Src_key, request.Src_bucket)

	if err != nil {
		return Response{
			Key:         "",
			ContentType: "",
			Data:        "",
			Status:      false,
			Error_msg:   "File " + request.Src_key + " not exists in " + request.Src_bucket + "",
		}, err
	}

	is_image, content_type := checkIsImage(source_file_path)
	if err == nil && is_image {
		prew_file, err := resizeImage(source_file_path, request.Img_width)
		if err != nil {
			return Response{
				Key:         "",
				ContentType: "",
				Data:        "",
				Status:      false,
				Error_msg:   "File " + request.Src_key + " is not image:" + content_type + " OR resize error",
			}, err
		}

		new_key, err = uploadS3Prew(prew_file, content_type, request.Src_bucket, request.Rewrite_part, request.Dst_bucket, request.Root_folder, request.Preset_name)
		if err != nil {
			return Response{
				Key:         "",
				ContentType: "",
				Data:        "",
				Status:      false,
				Error_msg:   "Unable to upload " + request.Dst_bucket + "",
			}, err
		}
		os.Remove(source_file_path)
		os.Remove(prew_file)
	}

	return Response{
		Key:         new_key.Key,
		ContentType: new_key.ContentType,
		Data:        new_key.Data,
		Status:      true,
		Error_msg:   "",
	}, nil
}

func getSource(sr_key string, sr_bucket string) (string, error) {
	local_file := tmp + sr_bucket + "/" + sr_key

	// create folder structure
	dir := filepath.Dir(local_file)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return "", err
	}

	// create file
	f, err := os.Create(local_file)
	if err != nil {
		return "", err
	}
	defer f.Close()

	_, err = downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(sr_bucket),
		Key:    aws.String(sr_key),
	})

	if err != nil {
		return "", errors.New("can't download source")
	}

	return local_file, nil
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func checkIsImage(f_path string) (bool, string) {

	buf, _ := ioutil.ReadFile(f_path)

	kind, unknown := filetype.Match(buf)
	if unknown != nil {
		return false, ""
	}

	if kind.MIME.Value != "image/jpeg" && kind.MIME.Value != "image/png" {
		return false, ""
	}

	return true, kind.MIME.Value
}

func resizeImage(f_path string, width int) (string, error) {
	img, err := imaging.Open(f_path)
	if err != nil {
		return "", err
	}

	// preparing
	dir, file := filepath.Split(f_path)
	dst_image := dir + "tmp_prew_" + file

	dst_exist, err := fileExists(dst_image)
	if dst_exist {
		r_err := os.Remove(dst_image)
		if r_err != nil {
			return "", r_err
		}
	}

	thumb := imaging.Resize(img, width, 0, imaging.Lanczos)

	rs_err := imaging.Save(thumb, dst_image, imaging.JPEGQuality(70))
	if rs_err != nil {
		return "", rs_err
	}

	return dst_image, nil
}

func uploadS3Prew(prew_file string, content_type string, src_bucket string, rewrite_part string, dst_bucket string, root_folder string, preset_name string) (Resize_result, error) {
	// upload thumbnail to S3 request.Dst_bucket, request.Root_folder, request.Preset_name
	up_file, err := os.Open(prew_file)
	if err != nil {
		return Resize_result{
			Key:         "",
			ContentType: "",
			Data:        "",
		}, err
	}
	defer up_file.Close()

	replacement := tmp + src_bucket + "/" + rewrite_part + "/"
	new_key_filename := strings.Replace(prew_file, replacement, "", -1)
	new_key := root_folder + "/" + preset_name + "/" + strings.Replace(new_key_filename, "tmp_prew_", "", -1)

	_, err = uploader.Upload(&s3manager.UploadInput{
		ACL:         aws.String("public-read"),
		Bucket:      aws.String(dst_bucket),
		Key:         aws.String(new_key),
		ContentType: aws.String(content_type),
		Body:        up_file,
	})

	if err != nil {
		return Resize_result{
			Key:         "",
			ContentType: "",
			Data:        "",
		}, err
	}

	// reading and return
	fi, err := up_file.Stat()
	size := fi.Size()
	buf := make([]byte, size)
	fReader := bufio.NewReader(up_file)
	fReader.Read(buf)
	imgBase64Str := base64.StdEncoding.EncodeToString(buf)

	return Resize_result{
		Key:         new_key,
		ContentType: content_type,
		Data:        imgBase64Str,
	}, nil
}