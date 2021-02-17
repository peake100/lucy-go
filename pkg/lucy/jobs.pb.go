// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.22.0
// 	protoc        v3.13.0
// source: lucy_proto/jobs.proto

package lucy

import (
	proto "github.com/golang/protobuf/proto"
	any "github.com/golang/protobuf/ptypes/any"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	cerealMessages "github.com/illuscio-dev/protoCereal-go/cerealMessages"
	protogen "github.com/peake100/gRPEAKEC-go/pkerr/protogen"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

// Status represent the current state of the job.
type Status int32

const (
	// PENDING - The Job / JobStage is not yet being run.
	Status_PENDING Status = 0
	// CANCELLED - The Job has been cancelled.
	Status_CANCELLED Status = 1
	// RUNNING - The Job / JobStage is currently being run.
	Status_RUNNING Status = 2
	// COMPLETED - The Job / JobStage is complete.
	Status_COMPLETED Status = 3
)

// Enum value maps for Status.
var (
	Status_name = map[int32]string{
		0: "PENDING",
		1: "CANCELLED",
		2: "RUNNING",
		3: "COMPLETED",
	}
	Status_value = map[string]int32{
		"PENDING":   0,
		"CANCELLED": 1,
		"RUNNING":   2,
		"COMPLETED": 3,
	}
)

func (x Status) Enum() *Status {
	p := new(Status)
	*p = x
	return p
}

func (x Status) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Status) Descriptor() protoreflect.EnumDescriptor {
	return file_lucy_proto_jobs_proto_enumTypes[0].Descriptor()
}

func (Status) Type() protoreflect.EnumType {
	return &file_lucy_proto_jobs_proto_enumTypes[0]
}

func (x Status) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Status.Descriptor instead.
func (Status) EnumDescriptor() ([]byte, []int) {
	return file_lucy_proto_jobs_proto_rawDescGZIP(), []int{0}
}

// Result details the outcome of a Job / JobStage. This value is made as an enum to
// enable additional future results like "PARTIAL_SUCCESS"
type Result int32

const (
	// NONE indicates that a job does not have a result yet/
	Result_NONE Result = 0
	// FAILED indicates that a job failed.
	Result_FAILED Result = 1
	// SUCCEEDED indicates that a job was successful.
	Result_SUCCEEDED Result = 2
)

// Enum value maps for Result.
var (
	Result_name = map[int32]string{
		0: "NONE",
		1: "FAILED",
		2: "SUCCEEDED",
	}
	Result_value = map[string]int32{
		"NONE":      0,
		"FAILED":    1,
		"SUCCEEDED": 2,
	}
)

func (x Result) Enum() *Result {
	p := new(Result)
	*p = x
	return p
}

func (x Result) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Result) Descriptor() protoreflect.EnumDescriptor {
	return file_lucy_proto_jobs_proto_enumTypes[1].Descriptor()
}

func (Result) Type() protoreflect.EnumType {
	return &file_lucy_proto_jobs_proto_enumTypes[1]
}

func (x Result) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Result.Descriptor instead.
func (Result) EnumDescriptor() ([]byte, []int) {
	return file_lucy_proto_jobs_proto_rawDescGZIP(), []int{1}
}

// JobStage is one discreet stage of work required to complete a job.
type JobStage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// type a human-readable identifier for the stage of work to be created. This value
	// must be unique within a job.
	// @inject_tag: bson:"type"
	Type string `protobuf:"bytes,1,opt,name=type,proto3" json:"type,omitempty" bson:"type"`
	// description is the human-readable description of the job.
	// @inject_tag: bson:"description"
	Description string `protobuf:"bytes,2,opt,name=description,proto3" json:"description,omitempty" bson:"description"`
	// Status is the status of the job stage.
	// @inject_tag: bson:"status"
	Status Status `protobuf:"varint,3,opt,name=status,proto3,enum=lucy.Status" json:"status,omitempty" bson:"status"`
	// run_by is the name of the worker who ran this JobStage.
	// @inject_tag: bson:"run_by"
	RunBy string `protobuf:"bytes,4,opt,name=run_by,json=runBy,proto3" json:"run_by,omitempty" bson:"run_by"`
	// started is the time the job stage was started.
	// @inject_tag: bson:"started"
	Started *timestamp.Timestamp `protobuf:"bytes,5,opt,name=started,proto3" json:"started,omitempty" bson:"started"`
	// completed is the time the job stage was completed.
	// @inject_tag: bson:"completed"
	Completed *timestamp.Timestamp `protobuf:"bytes,6,opt,name=completed,proto3" json:"completed,omitempty" bson:"completed"`
	// progress contains the approximate progress of the job from 0.0 - 1.0.
	// @inject_tag: bson:"progress"
	Progress float32 `protobuf:"fixed32,7,opt,name=progress,proto3" json:"progress,omitempty" bson:"progress"`
	// result is whether the job was successful.
	// @inject_tag: bson:"result"
	Result Result `protobuf:"varint,8,opt,name=result,proto3,enum=lucy.Result" json:"result,omitempty" bson:"result"`
	// error contains error information about the job stage.
	// @inject_tag: bson:"error"
	Error *protogen.Error `protobuf:"bytes,9,opt,name=error,proto3" json:"error,omitempty" bson:"error"`
	// result_data contains job completion data from the worker.
	// @inject_tag: bson:"result_data"
	ResultData *any.Any `protobuf:"bytes,10,opt,name=result_data,json=resultData,proto3" json:"result_data,omitempty" bson:"result_data"`
	// run_count is the number of times this job stage has been run.
	// @inject_tag: bson:"run_count"
	RunCount uint32 `protobuf:"varint,11,opt,name=run_count,json=runCount,proto3" json:"run_count,omitempty" bson:"run_count"`
}

func (x *JobStage) Reset() {
	*x = JobStage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_lucy_proto_jobs_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *JobStage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JobStage) ProtoMessage() {}

func (x *JobStage) ProtoReflect() protoreflect.Message {
	mi := &file_lucy_proto_jobs_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JobStage.ProtoReflect.Descriptor instead.
func (*JobStage) Descriptor() ([]byte, []int) {
	return file_lucy_proto_jobs_proto_rawDescGZIP(), []int{0}
}

func (x *JobStage) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *JobStage) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *JobStage) GetStatus() Status {
	if x != nil {
		return x.Status
	}
	return Status_PENDING
}

func (x *JobStage) GetRunBy() string {
	if x != nil {
		return x.RunBy
	}
	return ""
}

func (x *JobStage) GetStarted() *timestamp.Timestamp {
	if x != nil {
		return x.Started
	}
	return nil
}

func (x *JobStage) GetCompleted() *timestamp.Timestamp {
	if x != nil {
		return x.Completed
	}
	return nil
}

func (x *JobStage) GetProgress() float32 {
	if x != nil {
		return x.Progress
	}
	return 0
}

func (x *JobStage) GetResult() Result {
	if x != nil {
		return x.Result
	}
	return Result_NONE
}

func (x *JobStage) GetError() *protogen.Error {
	if x != nil {
		return x.Error
	}
	return nil
}

func (x *JobStage) GetResultData() *any.Any {
	if x != nil {
		return x.ResultData
	}
	return nil
}

func (x *JobStage) GetRunCount() uint32 {
	if x != nil {
		return x.RunCount
	}
	return 0
}

// Job is a series of JobStage values that together make up a single pipeline of work
// to be sequentially executed.
type Job struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// id is a UUID for the job
	// @inject_tag: bson:"id"
	Id *cerealMessages.UUID `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty" bson:"id"`
	// created is the time the job was created.
	// @inject_tag: bson:"created"
	Created *timestamp.Timestamp `protobuf:"bytes,2,opt,name=created,proto3" json:"created,omitempty" bson:"created"`
	// modified is the time the last modification to the job was made.
	// @inject_tag: bson:"modified"
	Modified *timestamp.Timestamp `protobuf:"bytes,3,opt,name=modified,proto3" json:"modified,omitempty" bson:"modified"`
	// human readable value that uniquely identifies this type of job for workers to
	// filter by.
	// @inject_tag: bson:"type"
	Type string `protobuf:"bytes,4,opt,name=type,proto3" json:"type,omitempty" bson:"type"`
	// name is the human-readable name of the job.
	// @inject_tag: bson:"name"
	Name string `protobuf:"bytes,5,opt,name=name,proto3" json:"name,omitempty" bson:"name"`
	// description is a human-readable description of the job stage.
	// @inject_tag: bson:"description"
	Description string `protobuf:"bytes,6,opt,name=description,proto3" json:"description,omitempty" bson:"description"`
	// input contains all the settings and data necessary for workers to complete the job.
	// @inject_tag: bson:"input"
	Input *any.Any `protobuf:"bytes,7,opt,name=input,proto3" json:"input,omitempty" bson:"input"`
	// max_retries is the maximum number of times this job can be retried. If 0, no
	// retries are allowed. Unbounded retries are currently unsupported.
	// @inject_tag: bson:"max_retries"
	MaxRetries uint32 `protobuf:"varint,8,opt,name=max_retries,json=maxRetries,proto3" json:"max_retries,omitempty" bson:"max_retries"`
	// stages contains all the stages of the job.
	// @inject_tag: bson:"stages"
	Stages []*JobStage `protobuf:"bytes,9,rep,name=stages,proto3" json:"stages,omitempty" bson:"stages"`
	// status contains the current state of the job.
	// @inject_tag: bson:"status"
	Status Status `protobuf:"varint,10,opt,name=status,proto3,enum=lucy.Status" json:"status,omitempty" bson:"status"`
	// the total progress of the job from 0.0 - 1.0.
	// @inject_tag: bson:"progress"
	Progress float32 `protobuf:"fixed32,11,opt,name=progress,proto3" json:"progress,omitempty" bson:"progress"`
	// result contains whether job succeeded or failed.
	// @inject_tag: bson:"result"
	Result Result `protobuf:"varint,12,opt,name=result,proto3,enum=lucy.Result" json:"result,omitempty" bson:"result"`
	// Runs is the number of times this job has been run.
	// @inject_tag: bson:"run_count"
	RunCount uint32 `protobuf:"varint,13,opt,name=run_count,json=runCount,proto3" json:"run_count,omitempty" bson:"run_count"`
}

func (x *Job) Reset() {
	*x = Job{}
	if protoimpl.UnsafeEnabled {
		mi := &file_lucy_proto_jobs_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Job) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Job) ProtoMessage() {}

func (x *Job) ProtoReflect() protoreflect.Message {
	mi := &file_lucy_proto_jobs_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Job.ProtoReflect.Descriptor instead.
func (*Job) Descriptor() ([]byte, []int) {
	return file_lucy_proto_jobs_proto_rawDescGZIP(), []int{1}
}

func (x *Job) GetId() *cerealMessages.UUID {
	if x != nil {
		return x.Id
	}
	return nil
}

func (x *Job) GetCreated() *timestamp.Timestamp {
	if x != nil {
		return x.Created
	}
	return nil
}

func (x *Job) GetModified() *timestamp.Timestamp {
	if x != nil {
		return x.Modified
	}
	return nil
}

func (x *Job) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *Job) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Job) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *Job) GetInput() *any.Any {
	if x != nil {
		return x.Input
	}
	return nil
}

func (x *Job) GetMaxRetries() uint32 {
	if x != nil {
		return x.MaxRetries
	}
	return 0
}

func (x *Job) GetStages() []*JobStage {
	if x != nil {
		return x.Stages
	}
	return nil
}

func (x *Job) GetStatus() Status {
	if x != nil {
		return x.Status
	}
	return Status_PENDING
}

func (x *Job) GetProgress() float32 {
	if x != nil {
		return x.Progress
	}
	return 0
}

func (x *Job) GetResult() Result {
	if x != nil {
		return x.Result
	}
	return Result_NONE
}

func (x *Job) GetRunCount() uint32 {
	if x != nil {
		return x.RunCount
	}
	return 0
}

// Batch is a collection of Job values that are tied together as a collection of work.
// Jobs contained within a batch are safe to concurrently execute.
type Batch struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// id is a UUID for the job.
	// @inject_tag: bson:"id"
	Id *cerealMessages.UUID `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty" bson:"id"`
	// created is the time the job was created.
	// @inject_tag: bson:"created"
	Created *timestamp.Timestamp `protobuf:"bytes,2,opt,name=created,proto3" json:"created,omitempty" bson:"created"`
	// modified is the time the last modification to the job was made.
	// @inject_tag: bson:"modified"
	Modified *timestamp.Timestamp `protobuf:"bytes,3,opt,name=modified,proto3" json:"modified,omitempty" bson:"modified"`
	// human readable value that uniquely identifies this type of batch for workers to
	// filter by.
	// @inject_tag: bson:"type"
	Type string `protobuf:"bytes,4,opt,name=type,proto3" json:"type,omitempty" bson:"type"`
	// name is the human-readable name of the batch.
	// @inject_tag: bson:"name"
	Name string `protobuf:"bytes,5,opt,name=name,proto3" json:"name,omitempty" bson:"name"`
	// description is the human-readable description of the batch.
	// @inject_tag: bson:"description"
	Description string `protobuf:"bytes,6,opt,name=description,proto3" json:"description,omitempty" bson:"description"`
	// @inject_tag: bson:"-"
	Jobs []*cerealMessages.UUID `protobuf:"bytes,7,rep,name=jobs,proto3" json:"jobs,omitempty" bson:"-"`
	// job_count is the number of jobs that are part of this batch.
	// @inject_tag: bson:"job_count"
	JobCount uint32 `protobuf:"varint,8,opt,name=job_count,json=jobCount,proto3" json:"job_count,omitempty" bson:"job_count"`
	// progress is the total progress on this batch from 0.0 - 1.0.
	// @inject_tag: bson:"progress"
	Progress float32 `protobuf:"fixed32,9,opt,name=progress,proto3" json:"progress,omitempty" bson:"progress"`
	// pending_count is the number pending jobs in this batch.
	// @inject_tag: bson:"pending_count"
	PendingCount uint32 `protobuf:"varint,10,opt,name=pending_count,json=pendingCount,proto3" json:"pending_count,omitempty" bson:"pending_count"`
	// cancelled_count is the number cancelled jobs in this batch.
	// @inject_tag: bson:"cancelled_count"
	CancelledCount uint32 `protobuf:"varint,11,opt,name=cancelled_count,json=cancelledCount,proto3" json:"cancelled_count,omitempty" bson:"cancelled_count"`
	// running_count is the number running jobs in this batch.
	// @inject_tag: bson:"running_count"
	RunningCount uint32 `protobuf:"varint,12,opt,name=running_count,json=runningCount,proto3" json:"running_count,omitempty" bson:"running_count"`
	// completed_count is the number completed jobs in this batch.
	// @inject_tag: bson:"completed_count"
	CompletedCount uint32 `protobuf:"varint,13,opt,name=completed_count,json=completedCount,proto3" json:"completed_count,omitempty" bson:"completed_count"`
	// success_count is the number of successes this batch
	// @inject_tag: bson:"success_count"
	SuccessCount uint32 `protobuf:"varint,14,opt,name=success_count,json=successCount,proto3" json:"success_count,omitempty" bson:"success_count"`
	// failure count is the number of failures in this batch.
	// @inject_tag: bson:"failure_count"
	FailureCount uint32 `protobuf:"varint,15,opt,name=failure_count,json=failureCount,proto3" json:"failure_count,omitempty" bson:"failure_count"`
}

func (x *Batch) Reset() {
	*x = Batch{}
	if protoimpl.UnsafeEnabled {
		mi := &file_lucy_proto_jobs_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Batch) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Batch) ProtoMessage() {}

func (x *Batch) ProtoReflect() protoreflect.Message {
	mi := &file_lucy_proto_jobs_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Batch.ProtoReflect.Descriptor instead.
func (*Batch) Descriptor() ([]byte, []int) {
	return file_lucy_proto_jobs_proto_rawDescGZIP(), []int{2}
}

func (x *Batch) GetId() *cerealMessages.UUID {
	if x != nil {
		return x.Id
	}
	return nil
}

func (x *Batch) GetCreated() *timestamp.Timestamp {
	if x != nil {
		return x.Created
	}
	return nil
}

func (x *Batch) GetModified() *timestamp.Timestamp {
	if x != nil {
		return x.Modified
	}
	return nil
}

func (x *Batch) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *Batch) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Batch) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *Batch) GetJobs() []*cerealMessages.UUID {
	if x != nil {
		return x.Jobs
	}
	return nil
}

func (x *Batch) GetJobCount() uint32 {
	if x != nil {
		return x.JobCount
	}
	return 0
}

func (x *Batch) GetProgress() float32 {
	if x != nil {
		return x.Progress
	}
	return 0
}

func (x *Batch) GetPendingCount() uint32 {
	if x != nil {
		return x.PendingCount
	}
	return 0
}

func (x *Batch) GetCancelledCount() uint32 {
	if x != nil {
		return x.CancelledCount
	}
	return 0
}

func (x *Batch) GetRunningCount() uint32 {
	if x != nil {
		return x.RunningCount
	}
	return 0
}

func (x *Batch) GetCompletedCount() uint32 {
	if x != nil {
		return x.CompletedCount
	}
	return 0
}

func (x *Batch) GetSuccessCount() uint32 {
	if x != nil {
		return x.SuccessCount
	}
	return 0
}

func (x *Batch) GetFailureCount() uint32 {
	if x != nil {
		return x.FailureCount
	}
	return 0
}

// BatchJobs holds the job details for a given batch.
type BatchJobs struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// jobs is the list og jobs for a batch.
	// @inject_tag: bson:"jobs"
	Jobs []*Job `protobuf:"bytes,1,rep,name=jobs,proto3" json:"jobs,omitempty" bson:"jobs"`
}

func (x *BatchJobs) Reset() {
	*x = BatchJobs{}
	if protoimpl.UnsafeEnabled {
		mi := &file_lucy_proto_jobs_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchJobs) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchJobs) ProtoMessage() {}

func (x *BatchJobs) ProtoReflect() protoreflect.Message {
	mi := &file_lucy_proto_jobs_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchJobs.ProtoReflect.Descriptor instead.
func (*BatchJobs) Descriptor() ([]byte, []int) {
	return file_lucy_proto_jobs_proto_rawDescGZIP(), []int{3}
}

func (x *BatchJobs) GetJobs() []*Job {
	if x != nil {
		return x.Jobs
	}
	return nil
}

var File_lucy_proto_jobs_proto protoreflect.FileDescriptor

var file_lucy_proto_jobs_proto_rawDesc = []byte{
	0x0a, 0x15, 0x6c, 0x75, 0x63, 0x79, 0x5f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x6a, 0x6f, 0x62,
	0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04, 0x6c, 0x75, 0x63, 0x79, 0x1a, 0x17, 0x63,
	0x65, 0x72, 0x65, 0x61, 0x6c, 0x5f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x75, 0x75, 0x69, 0x64,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x19, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x61, 0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x20, 0x67, 0x72, 0x70, 0x65, 0x61, 0x6b, 0x65, 0x63, 0x5f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2f, 0x70, 0x6b, 0x65, 0x72, 0x72, 0x2f, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x22, 0xa7, 0x03, 0x0a, 0x08, 0x4a, 0x6f, 0x62, 0x53, 0x74, 0x61, 0x67,
	0x65, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x04, 0x74, 0x79, 0x70, 0x65, 0x12, 0x20, 0x0a, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70,
	0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x64, 0x65, 0x73, 0x63,
	0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x24, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75,
	0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0c, 0x2e, 0x6c, 0x75, 0x63, 0x79, 0x2e, 0x53,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x15, 0x0a,
	0x06, 0x72, 0x75, 0x6e, 0x5f, 0x62, 0x79, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x72,
	0x75, 0x6e, 0x42, 0x79, 0x12, 0x34, 0x0a, 0x07, 0x73, 0x74, 0x61, 0x72, 0x74, 0x65, 0x64, 0x18,
	0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x52, 0x07, 0x73, 0x74, 0x61, 0x72, 0x74, 0x65, 0x64, 0x12, 0x38, 0x0a, 0x09, 0x63, 0x6f,
	0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x63, 0x6f, 0x6d, 0x70, 0x6c,
	0x65, 0x74, 0x65, 0x64, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x72, 0x6f, 0x67, 0x72, 0x65, 0x73, 0x73,
	0x18, 0x07, 0x20, 0x01, 0x28, 0x02, 0x52, 0x08, 0x70, 0x72, 0x6f, 0x67, 0x72, 0x65, 0x73, 0x73,
	0x12, 0x24, 0x0a, 0x06, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x18, 0x08, 0x20, 0x01, 0x28, 0x0e,
	0x32, 0x0c, 0x2e, 0x6c, 0x75, 0x63, 0x79, 0x2e, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x52, 0x06,
	0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x22, 0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18,
	0x09, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0c, 0x2e, 0x70, 0x6b, 0x65, 0x72, 0x72, 0x2e, 0x45, 0x72,
	0x72, 0x6f, 0x72, 0x52, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x35, 0x0a, 0x0b, 0x72, 0x65,
	0x73, 0x75, 0x6c, 0x74, 0x5f, 0x64, 0x61, 0x74, 0x61, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x0a, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x44, 0x61, 0x74,
	0x61, 0x12, 0x1b, 0x0a, 0x09, 0x72, 0x75, 0x6e, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x0b,
	0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x72, 0x75, 0x6e, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x22, 0xd5,
	0x03, 0x0a, 0x03, 0x4a, 0x6f, 0x62, 0x12, 0x1c, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x0c, 0x2e, 0x63, 0x65, 0x72, 0x65, 0x61, 0x6c, 0x2e, 0x55, 0x55, 0x49, 0x44,
	0x52, 0x02, 0x69, 0x64, 0x12, 0x34, 0x0a, 0x07, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x52, 0x07, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x12, 0x36, 0x0a, 0x08, 0x6d, 0x6f,
	0x64, 0x69, 0x66, 0x69, 0x65, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x08, 0x6d, 0x6f, 0x64, 0x69, 0x66, 0x69,
	0x65, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x20, 0x0a, 0x0b, 0x64, 0x65,
	0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x2a, 0x0a, 0x05,
	0x69, 0x6e, 0x70, 0x75, 0x74, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f,
	0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e,
	0x79, 0x52, 0x05, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x12, 0x1f, 0x0a, 0x0b, 0x6d, 0x61, 0x78, 0x5f,
	0x72, 0x65, 0x74, 0x72, 0x69, 0x65, 0x73, 0x18, 0x08, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0a, 0x6d,
	0x61, 0x78, 0x52, 0x65, 0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x26, 0x0a, 0x06, 0x73, 0x74, 0x61,
	0x67, 0x65, 0x73, 0x18, 0x09, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x6c, 0x75, 0x63, 0x79,
	0x2e, 0x4a, 0x6f, 0x62, 0x53, 0x74, 0x61, 0x67, 0x65, 0x52, 0x06, 0x73, 0x74, 0x61, 0x67, 0x65,
	0x73, 0x12, 0x24, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x0a, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x0c, 0x2e, 0x6c, 0x75, 0x63, 0x79, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52,
	0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x72, 0x6f, 0x67, 0x72,
	0x65, 0x73, 0x73, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x02, 0x52, 0x08, 0x70, 0x72, 0x6f, 0x67, 0x72,
	0x65, 0x73, 0x73, 0x12, 0x24, 0x0a, 0x06, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x18, 0x0c, 0x20,
	0x01, 0x28, 0x0e, 0x32, 0x0c, 0x2e, 0x6c, 0x75, 0x63, 0x79, 0x2e, 0x52, 0x65, 0x73, 0x75, 0x6c,
	0x74, 0x52, 0x06, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x1b, 0x0a, 0x09, 0x72, 0x75, 0x6e,
	0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x0d, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x72, 0x75,
	0x6e, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x22, 0x9e, 0x04, 0x0a, 0x05, 0x42, 0x61, 0x74, 0x63, 0x68,
	0x12, 0x1c, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0c, 0x2e, 0x63,
	0x65, 0x72, 0x65, 0x61, 0x6c, 0x2e, 0x55, 0x55, 0x49, 0x44, 0x52, 0x02, 0x69, 0x64, 0x12, 0x34,
	0x0a, 0x07, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x07, 0x63, 0x72, 0x65,
	0x61, 0x74, 0x65, 0x64, 0x12, 0x36, 0x0a, 0x08, 0x6d, 0x6f, 0x64, 0x69, 0x66, 0x69, 0x65, 0x64,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x52, 0x08, 0x6d, 0x6f, 0x64, 0x69, 0x66, 0x69, 0x65, 0x64, 0x12, 0x12, 0x0a, 0x04,
	0x74, 0x79, 0x70, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x74, 0x79, 0x70, 0x65,
	0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x6e, 0x61, 0x6d, 0x65, 0x12, 0x20, 0x0a, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74,
	0x69, 0x6f, 0x6e, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72,
	0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x20, 0x0a, 0x04, 0x6a, 0x6f, 0x62, 0x73, 0x18, 0x07,
	0x20, 0x03, 0x28, 0x0b, 0x32, 0x0c, 0x2e, 0x63, 0x65, 0x72, 0x65, 0x61, 0x6c, 0x2e, 0x55, 0x55,
	0x49, 0x44, 0x52, 0x04, 0x6a, 0x6f, 0x62, 0x73, 0x12, 0x1b, 0x0a, 0x09, 0x6a, 0x6f, 0x62, 0x5f,
	0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x08, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x6a, 0x6f, 0x62,
	0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x72, 0x6f, 0x67, 0x72, 0x65, 0x73,
	0x73, 0x18, 0x09, 0x20, 0x01, 0x28, 0x02, 0x52, 0x08, 0x70, 0x72, 0x6f, 0x67, 0x72, 0x65, 0x73,
	0x73, 0x12, 0x23, 0x0a, 0x0d, 0x70, 0x65, 0x6e, 0x64, 0x69, 0x6e, 0x67, 0x5f, 0x63, 0x6f, 0x75,
	0x6e, 0x74, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0c, 0x70, 0x65, 0x6e, 0x64, 0x69, 0x6e,
	0x67, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x27, 0x0a, 0x0f, 0x63, 0x61, 0x6e, 0x63, 0x65, 0x6c,
	0x6c, 0x65, 0x64, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x0d, 0x52,
	0x0e, 0x63, 0x61, 0x6e, 0x63, 0x65, 0x6c, 0x6c, 0x65, 0x64, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12,
	0x23, 0x0a, 0x0d, 0x72, 0x75, 0x6e, 0x6e, 0x69, 0x6e, 0x67, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74,
	0x18, 0x0c, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0c, 0x72, 0x75, 0x6e, 0x6e, 0x69, 0x6e, 0x67, 0x43,
	0x6f, 0x75, 0x6e, 0x74, 0x12, 0x27, 0x0a, 0x0f, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65,
	0x64, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x0d, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0e, 0x63,
	0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x23, 0x0a,
	0x0d, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x0e,
	0x20, 0x01, 0x28, 0x0d, 0x52, 0x0c, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x43, 0x6f, 0x75,
	0x6e, 0x74, 0x12, 0x23, 0x0a, 0x0d, 0x66, 0x61, 0x69, 0x6c, 0x75, 0x72, 0x65, 0x5f, 0x63, 0x6f,
	0x75, 0x6e, 0x74, 0x18, 0x0f, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0c, 0x66, 0x61, 0x69, 0x6c, 0x75,
	0x72, 0x65, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x22, 0x2a, 0x0a, 0x09, 0x42, 0x61, 0x74, 0x63, 0x68,
	0x4a, 0x6f, 0x62, 0x73, 0x12, 0x1d, 0x0a, 0x04, 0x6a, 0x6f, 0x62, 0x73, 0x18, 0x01, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x09, 0x2e, 0x6c, 0x75, 0x63, 0x79, 0x2e, 0x4a, 0x6f, 0x62, 0x52, 0x04, 0x6a,
	0x6f, 0x62, 0x73, 0x2a, 0x40, 0x0a, 0x06, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x0b, 0x0a,
	0x07, 0x50, 0x45, 0x4e, 0x44, 0x49, 0x4e, 0x47, 0x10, 0x00, 0x12, 0x0d, 0x0a, 0x09, 0x43, 0x41,
	0x4e, 0x43, 0x45, 0x4c, 0x4c, 0x45, 0x44, 0x10, 0x01, 0x12, 0x0b, 0x0a, 0x07, 0x52, 0x55, 0x4e,
	0x4e, 0x49, 0x4e, 0x47, 0x10, 0x02, 0x12, 0x0d, 0x0a, 0x09, 0x43, 0x4f, 0x4d, 0x50, 0x4c, 0x45,
	0x54, 0x45, 0x44, 0x10, 0x03, 0x2a, 0x2d, 0x0a, 0x06, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12,
	0x08, 0x0a, 0x04, 0x4e, 0x4f, 0x4e, 0x45, 0x10, 0x00, 0x12, 0x0a, 0x0a, 0x06, 0x46, 0x41, 0x49,
	0x4c, 0x45, 0x44, 0x10, 0x01, 0x12, 0x0d, 0x0a, 0x09, 0x53, 0x55, 0x43, 0x43, 0x45, 0x45, 0x44,
	0x45, 0x44, 0x10, 0x02, 0x42, 0x22, 0x5a, 0x20, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x70, 0x65, 0x61, 0x6b, 0x65, 0x31, 0x30, 0x30, 0x2f, 0x6c, 0x75, 0x63, 0x79,
	0x2d, 0x67, 0x6f, 0x2f, 0x6c, 0x75, 0x63, 0x79, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_lucy_proto_jobs_proto_rawDescOnce sync.Once
	file_lucy_proto_jobs_proto_rawDescData = file_lucy_proto_jobs_proto_rawDesc
)

func file_lucy_proto_jobs_proto_rawDescGZIP() []byte {
	file_lucy_proto_jobs_proto_rawDescOnce.Do(func() {
		file_lucy_proto_jobs_proto_rawDescData = protoimpl.X.CompressGZIP(file_lucy_proto_jobs_proto_rawDescData)
	})
	return file_lucy_proto_jobs_proto_rawDescData
}

var file_lucy_proto_jobs_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_lucy_proto_jobs_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_lucy_proto_jobs_proto_goTypes = []interface{}{
	(Status)(0),                 // 0: lucy.Status
	(Result)(0),                 // 1: lucy.Result
	(*JobStage)(nil),            // 2: lucy.JobStage
	(*Job)(nil),                 // 3: lucy.Job
	(*Batch)(nil),               // 4: lucy.Batch
	(*BatchJobs)(nil),           // 5: lucy.BatchJobs
	(*timestamp.Timestamp)(nil), // 6: google.protobuf.Timestamp
	(*protogen.Error)(nil),      // 7: pkerr.Error
	(*any.Any)(nil),             // 8: google.protobuf.Any
	(*cerealMessages.UUID)(nil), // 9: cereal.UUID
}
var file_lucy_proto_jobs_proto_depIdxs = []int32{
	0,  // 0: lucy.JobStage.status:type_name -> lucy.Status
	6,  // 1: lucy.JobStage.started:type_name -> google.protobuf.Timestamp
	6,  // 2: lucy.JobStage.completed:type_name -> google.protobuf.Timestamp
	1,  // 3: lucy.JobStage.result:type_name -> lucy.Result
	7,  // 4: lucy.JobStage.error:type_name -> pkerr.Error
	8,  // 5: lucy.JobStage.result_data:type_name -> google.protobuf.Any
	9,  // 6: lucy.Job.id:type_name -> cereal.UUID
	6,  // 7: lucy.Job.created:type_name -> google.protobuf.Timestamp
	6,  // 8: lucy.Job.modified:type_name -> google.protobuf.Timestamp
	8,  // 9: lucy.Job.input:type_name -> google.protobuf.Any
	2,  // 10: lucy.Job.stages:type_name -> lucy.JobStage
	0,  // 11: lucy.Job.status:type_name -> lucy.Status
	1,  // 12: lucy.Job.result:type_name -> lucy.Result
	9,  // 13: lucy.Batch.id:type_name -> cereal.UUID
	6,  // 14: lucy.Batch.created:type_name -> google.protobuf.Timestamp
	6,  // 15: lucy.Batch.modified:type_name -> google.protobuf.Timestamp
	9,  // 16: lucy.Batch.jobs:type_name -> cereal.UUID
	3,  // 17: lucy.BatchJobs.jobs:type_name -> lucy.Job
	18, // [18:18] is the sub-list for method output_type
	18, // [18:18] is the sub-list for method input_type
	18, // [18:18] is the sub-list for extension type_name
	18, // [18:18] is the sub-list for extension extendee
	0,  // [0:18] is the sub-list for field type_name
}

func init() { file_lucy_proto_jobs_proto_init() }
func file_lucy_proto_jobs_proto_init() {
	if File_lucy_proto_jobs_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_lucy_proto_jobs_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*JobStage); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_lucy_proto_jobs_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Job); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_lucy_proto_jobs_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Batch); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_lucy_proto_jobs_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchJobs); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_lucy_proto_jobs_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_lucy_proto_jobs_proto_goTypes,
		DependencyIndexes: file_lucy_proto_jobs_proto_depIdxs,
		EnumInfos:         file_lucy_proto_jobs_proto_enumTypes,
		MessageInfos:      file_lucy_proto_jobs_proto_msgTypes,
	}.Build()
	File_lucy_proto_jobs_proto = out.File
	file_lucy_proto_jobs_proto_rawDesc = nil
	file_lucy_proto_jobs_proto_goTypes = nil
	file_lucy_proto_jobs_proto_depIdxs = nil
}