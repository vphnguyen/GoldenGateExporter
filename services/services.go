// Khai báo và khởi tạo các collector.
// Thu thập metric ở phần collect.
// Goi ham chuyen xml thanh Object
// Goi ham chuyen Object thanh Metric
// ==== Fix loi ko get dc.  Allow....... => Warning...
package services

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/vphnguyen/GoldenGateExporter/model"
	"github.com/vphnguyen/GoldenGateExporter/storage"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/relvacode/iso8601"
	log "github.com/sirupsen/logrus"
)

const collector = "GoldenGate"

var config model.Config

// Struct timestamp dạng iso8601.
type ExternalAPIResponse struct {
	Timestamp *iso8601.Time
}

// Khai bao cac Collector sẽ sử dụng.
type GoldenGateCollector struct {
	metricStatus            *prometheus.Desc
	metricTrailRba          *prometheus.Desc
	metricTrailSeq          *prometheus.Desc
	metricTrailIoWriteCount *prometheus.Desc
	metricTrailIoWriteByte  *prometheus.Desc
	metricTrailIoReadCount  *prometheus.Desc
	metricTrailIoReadByte   *prometheus.Desc
	metricTrailMaxBytes     *prometheus.Desc
	metricStatistics        *prometheus.Desc
	metricLastOperationLag  *prometheus.Desc
	metricLastOperationTs   *prometheus.Desc
	metricLastCheckpointTs  *prometheus.Desc
	metricInputCheckpoint   *prometheus.Desc
	//network-stats
	metricInbound_bytes     *prometheus.Desc
	metricInbound_messages  *prometheus.Desc
	metricOutbound_bytes    *prometheus.Desc
	metricOutbound_messages *prometheus.Desc
	metricSend_wait_time    *prometheus.Desc
	metricReceive_wait_time *prometheus.Desc
	metricSend_count        *prometheus.Desc
	metricReceive_count     *prometheus.Desc
}

// Khai bao cac describe
func (collector *GoldenGateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- collector.metricStatus
	ch <- collector.metricTrailRba
	ch <- collector.metricTrailSeq
	ch <- collector.metricTrailIoWriteCount
	ch <- collector.metricTrailIoWriteByte
	ch <- collector.metricTrailMaxBytes
	ch <- collector.metricTrailIoReadCount
	ch <- collector.metricTrailIoReadByte
	ch <- collector.metricStatistics
	ch <- collector.metricLastOperationLag
	ch <- collector.metricLastOperationTs
	ch <- collector.metricLastCheckpointTs
	ch <- collector.metricInputCheckpoint
	ch <- collector.metricInbound_bytes
	ch <- collector.metricInbound_messages
	ch <- collector.metricOutbound_bytes
	ch <- collector.metricOutbound_messages
	ch <- collector.metricSend_wait_time
	ch <- collector.metricReceive_wait_time
	ch <- collector.metricSend_count
	ch <- collector.metricReceive_count
}

// Định nghĩa các nội dung trong decribe
func NewGoldenGateCollector(c model.Config) *GoldenGateCollector {
	config = c
	return &GoldenGateCollector{
		// === STATUS & RBA + SEQ
		metricStatus: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "status"),
			"Status cua cac group trong GG _ type 2:Capture:EXTRACT 4:pump:EXTRACT 3:Delivery:REPLICAT 14:PMSRVR 1:MANAGER _status 3:running 6:stopped 8:append 1:Registered never executed",
			[]string{"mgr_host", "group_name", "type"}, nil,
		),
		// ==
		metricTrailSeq: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "trail_seq"),
			"So lan ma file trail da thuc hien rotate",
			[]string{"trail_name", "trail_path", "hostname", "group_name"}, nil,
		),
		metricTrailRba: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "trail_rba"),
			"Kich thuoc hien tai cua file trail dang hoat dong",
			[]string{"trail_name", "trail_path", "hostname", "group_name"}, nil,
		),
		// == WRITE
		metricTrailIoWriteCount: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "io_write_count"),
			"So lan ghi du lieu vao cac file trail _ ap dung cho EXTRACT PUMP",
			[]string{"trail_name", "trail_path", "hostname", "group_name"}, nil,
		),
		metricTrailIoWriteByte: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "io_write_bytes"),
			"So byte da duoc ghi vao cac file trail _ ap dung cho EXTRACT PUMP",
			[]string{"trail_name", "trail_path", "hostname", "group_name"}, nil,
		),
		// == READ
		metricTrailIoReadCount: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "io_read_count"),
			"So lan doc du lieu tu cac file trail _ ap dung cho PUMP REP",
			[]string{"trail_name", "trail_path", "hostname", "group_name"}, nil,
		),
		metricTrailIoReadByte: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "io_read_bytes"),
			"So byte da doc tu cac file trail _ ap dung cho PUMP REP",
			[]string{"trail_name", "trail_path", "hostname", "group_name"}, nil,
		),
		//==== EXTRACT
		metricTrailMaxBytes: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "extract_trail_max_bytes"),
			"Trail Output _ extract_trail_max_bytes _ Kich thuoc toi da cua file trail",
			[]string{"trail_name", "trail_path", "hostname", "group_name"}, nil,
		),
		//==== PUMP
		metricStatistics: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "statistics"),
			"metricStatistics HELP",
			[]string{"hostname", "group_name", "mapped"}, nil,
		),
		metricLastOperationLag: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "last_operation_lag"),
			"last_operation_lag",
			[]string{"group_name"}, nil,
		),
		metricLastOperationTs: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "last_operation_ts"),
			"last_operation_ts",
			[]string{"group_name"}, nil,
		),
		metricLastCheckpointTs: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "last_checkpoint_ts"),
			"last_operation_ts metricLastCheckpointTs",
			[]string{"group_name"}, nil,
		),
		metricInputCheckpoint: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "input_checkpoint"),
			"input_checkpoint metricInputCheckpoint",
			[]string{"group_name"}, nil,
		),
		metricInbound_bytes: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "Inbound_bytes"),
			"Inbound_bytes metricInbound_bytes",
			[]string{"group_name"}, nil,
		),
		metricInbound_messages: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "Inbound_messages"),
			"Inbound_messages metricInbound_messages",
			[]string{"group_name"}, nil,
		),
		metricOutbound_bytes: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "Outbound_bytes"),
			"Outbound_bytes metricOutbound_bytes",
			[]string{"group_name"}, nil,
		),
		metricOutbound_messages: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "Outbound_messages"),
			"Outbound_messages metricOutbound_messages",
			[]string{"group_name"}, nil,
		),
		metricSend_wait_time: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "Send_wait_time"),
			"Send_wait_time metricSend_wait_time",
			[]string{"group_name"}, nil,
		),
		metricReceive_wait_time: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "Receive_wait_time"),
			"Receive_wait_time metricReceive_wait_time",
			[]string{"group_name"}, nil,
		),
		metricSend_count: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "Send_count"),
			"Send_count metricSend_count",
			[]string{"group_name"}, nil,
		),
		metricReceive_count: prometheus.NewDesc(
			prometheus.BuildFQName(collector, "", "Receive_count"),
			"Receive_count metricReceive_count",
			[]string{"group_name"}, nil,
		),
	}
}

// Chứa các hàm thu thập metric.
func (collector *GoldenGateCollector) Collect(ch chan<- prometheus.Metric) {
	var (
		manager           model.ManagerModel
		performanceServer model.PerformanceServerModel
		listOfExtract     []model.ExtractModel
		listOfPump        []model.PumpModel
		listOfReplicat    []model.ReplicatModel
	)
	log.Debugf("===== GET GROUPS ==================")
	mgroups, err := storage.GetGroups(config.RootURL)
	log.Debugf("Start getting info from: " + config.RootURL + "/groups")
	if err != nil {
		log.Errorf("Service - khong the parser Object - groups: %s", err)
	}
	for _, aGroup := range mgroups.GroupRefs {
		log.Debugf("GROUP: %s :%s", aGroup.Name, typeToString(aGroup.Type))
		if aGroup.IsExtract() {
			anExtract, er := storage.GetExtract(config.RootURL, aGroup.URL)
			if er != nil {
				log.Warnf("Service - %s", er)
				log.Infof("Skipped ")
			}
			if anExtract != nil && er == nil {
				listOfExtract = append(listOfExtract, *anExtract)
				continue
			}
		}
		if aGroup.IsPump() {
			aPump, er := storage.GetPump(config.RootURL, aGroup.URL)
			if er != nil {
				log.Warnf("Service - %s", er)
				log.Infof("Skipped ")
				continue
			}
			listOfPump = append(listOfPump, *aPump)
			continue
		}
		if aGroup.IsManager() {
			if er := storage.GetManager(config.RootURL, aGroup.URL, &manager); er != nil {
				log.Infof("Service - %s", er)
				continue
			}
			continue
		}
		if aGroup.IsPerformanceServer() {
			if er := storage.GetPerformanceServer(config.RootURL, aGroup.URL, &performanceServer); er != nil {
				log.Infof("Service - %s", er)
				continue
			}
			continue
		}
		if aGroup.IsReplicat() {
			aReplicat, er := storage.GetReplicat(config.RootURL, aGroup.URL)
			if er != nil {
				log.Infof("Service - %s", er)
				log.Infof("Skipped ")
				continue
			}
			listOfReplicat = append(listOfReplicat, *aReplicat)
			continue
		}

	}
	log.Debugf("===== Parse Metric ==================")
	GetMetrics(ch, collector, &manager, &performanceServer, &listOfExtract, &listOfPump, &listOfReplicat)
}

// Truyền các giá trị từ các object vào collector
func GetMetrics(ch chan<- prometheus.Metric, collector *GoldenGateCollector,
	manager *model.ManagerModel,
	performanceServer *model.PerformanceServerModel,
	listOfExtract *[]model.ExtractModel,
	listOfPump *[]model.PumpModel,
	listOfReplicat *[]model.ReplicatModel) {

	// ===== MGR        =======
	log.Debugf("Manager")
	log.Debugf("  - %s", manager.Name)
	ch <- prometheus.MustNewConstMetric(collector.metricStatus,
		prometheus.GaugeValue,
		toFloat64("", manager.Process.Status),
		[]string{config.MgrHost, manager.Process.Name, typeToString(manager.Process.Type)}...)
	// ===== Extract    =======

	log.Debugf("Extract")
	for _, extract := range *listOfExtract {
		log.Debugf("  - %s", extract.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricStatus,
			prometheus.GaugeValue,
			toFloat64("metricStatus", extract.Process.Status),
			[]string{config.MgrHost, extract.Process.Name, typeToString(extract.Process.Type)}...)
		ch <- prometheus.MustNewConstMetric(collector.metricLastOperationLag,
			prometheus.GaugeValue,
			toFloat64("metricLastOperationLag", extract.Process.PositionEr.LastOperationLag),
			extract.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricLastOperationTs,
			prometheus.GaugeValue,
			toUnixTime(extract.Process.PositionEr.LastOperationTs),
			extract.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricLastCheckpointTs,
			prometheus.GaugeValue,
			toUnixTime(extract.Process.PositionEr.LastCheckpointTs),
			extract.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricInputCheckpoint,
			prometheus.GaugeValue,
			getInputCheckPointValue(extract.Process.PositionEr.InputCheckpoint),
			extract.Process.Name)

		for _, trail := range extract.Process.TrailOutput {
			//========== io_write_count     "trail_name","trail_path","hostname","group_name"
			ch <- prometheus.MustNewConstMetric(collector.metricTrailIoWriteCount,
				prometheus.GaugeValue,
				toFloat64("metricTrailIoWriteCount", trail.IoWriteCount),
				[]string{trail.TrailName, trail.TrailPath, trail.Hostname, extract.Process.Name}...)
			//========== io_write_bytes
			ch <- prometheus.MustNewConstMetric(collector.metricTrailIoWriteByte,
				prometheus.GaugeValue,
				toFloat64("metricTrailIoWriteByte", trail.IoWriteBytes),
				[]string{trail.TrailName, trail.TrailPath, trail.Hostname, extract.Process.Name}...)
			//========== metricTrailRba
			ch <- prometheus.MustNewConstMetric(collector.metricTrailRba,
				prometheus.GaugeValue,
				toFloat64("metricTrailRba", trail.TrailRba),
				[]string{trail.TrailName, trail.TrailPath, trail.Hostname, extract.Process.Name}...)
			//========== metricTrailSeq
			ch <- prometheus.MustNewConstMetric(collector.metricTrailSeq,
				prometheus.GaugeValue,
				toFloat64("metricTrailSeq", trail.TrailSeq),
				[]string{trail.TrailName, trail.TrailPath, trail.Hostname, extract.Process.Name}...)
			//========== extract_metricTrailMaxBytes
			ch <- prometheus.MustNewConstMetric(collector.metricTrailMaxBytes,
				prometheus.GaugeValue,
				toFloat64("metricTrailMaxBytes", trail.TrailMaxBytes),
				[]string{trail.TrailName, trail.TrailPath, trail.Hostname, extract.Process.Name}...)
		}

		log.Debugf("\t- StatisticsExtract")
		a := reflect.ValueOf(&extract.Process.StatisticsExtract).Elem()
		for i := 0; i < (a.NumField()); i++ {
			if a.Type().Field(i).Name != "Text" {
				ch <- prometheus.MustNewConstMetric(collector.metricStatistics,
					prometheus.GaugeValue,
					toFloat64("metricStatistics."+a.Type().Field(i).Name, fmt.Sprintf("%s", a.Field(i).Interface())),
					[]string{config.MgrHost, extract.Process.Name, a.Type().Field(i).Name}...)
			}
		}
	}

	// ===== PUMP  =======
	log.Debugf("Pump")
	for _, pump := range *listOfPump {
		log.Debugf("  - %s", pump.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricStatus,
			prometheus.GaugeValue,
			toFloat64("metricStatus", pump.Process.Status),
			[]string{config.MgrHost, pump.Process.Name, typeToString(pump.Process.Type)}...)
		ch <- prometheus.MustNewConstMetric(collector.metricLastOperationLag,
			prometheus.GaugeValue,
			toFloat64("metricLastOperationLag", pump.Process.PositionEr.LastOperationLag),
			pump.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricLastOperationTs,
			prometheus.GaugeValue,
			toUnixTime(pump.Process.PositionEr.LastOperationTs),
			pump.Process.Name)

		// TCP stat
		ch <- prometheus.MustNewConstMetric(collector.metricInbound_bytes,
			prometheus.GaugeValue,
			toFloat64("metricInbound_bytes", pump.Process.NetworkStats.InboundBytes),
			pump.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricInbound_messages,
			prometheus.GaugeValue,
			toFloat64("metricInbound_messages", pump.Process.NetworkStats.InboundMessages),
			pump.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricOutbound_bytes,
			prometheus.GaugeValue,
			toFloat64("metricOutbound_bytes", pump.Process.NetworkStats.OutboundBytes),
			pump.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricOutbound_messages,
			prometheus.GaugeValue,
			toFloat64("metricOutbound_messages", pump.Process.NetworkStats.OutboundMessages),
			pump.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricSend_wait_time,
			prometheus.GaugeValue,
			toFloat64("metricSend_wait_time", pump.Process.NetworkStats.SendWaitTime),
			pump.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricReceive_wait_time,
			prometheus.GaugeValue,
			toFloat64("metricReceive_wait_time", pump.Process.NetworkStats.ReceiveWaitTime),
			pump.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricSend_count,
			prometheus.GaugeValue,
			toFloat64("metricSend_count", pump.Process.NetworkStats.SendCount),
			pump.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricReceive_count,
			prometheus.GaugeValue,
			toFloat64("metricReceive_count", pump.Process.NetworkStats.ReceiveCount),
			pump.Process.Name)

		// === Trail in
		// -- REad
		ch <- prometheus.MustNewConstMetric(collector.metricTrailIoReadCount,
			prometheus.GaugeValue,
			toFloat64("metricTrailIoReadCount", pump.Process.TrailInput.IoReadCount),
			[]string{pump.Process.TrailInput.TrailName, pump.Process.TrailInput.TrailPath, config.MgrHost, pump.Process.Name}...)
		ch <- prometheus.MustNewConstMetric(collector.metricTrailIoReadByte,
			prometheus.GaugeValue,
			toFloat64("metricTrailIoReadByte", pump.Process.TrailInput.IoReadBytes),
			[]string{pump.Process.TrailInput.TrailName, pump.Process.TrailInput.TrailPath, config.MgrHost, pump.Process.Name}...)
		// -- RBA - SEQ
		ch <- prometheus.MustNewConstMetric(collector.metricTrailRba,
			prometheus.GaugeValue,
			toFloat64("metricTrailRba", pump.Process.TrailInput.TrailRba),
			[]string{pump.Process.TrailInput.TrailName, pump.Process.TrailInput.TrailPath, config.MgrHost, pump.Process.Name}...)

		ch <- prometheus.MustNewConstMetric(collector.metricTrailSeq,
			prometheus.GaugeValue,
			toFloat64("metricTrailSeq", pump.Process.TrailInput.TrailSeq),
			[]string{pump.Process.TrailInput.TrailName, pump.Process.TrailInput.TrailPath, config.MgrHost, pump.Process.Name}...)
		// === Trail out (s)
		for _, trailout := range pump.Process.TrailOutput {
			// -- WRITE
			ch <- prometheus.MustNewConstMetric(collector.metricTrailIoWriteCount,
				prometheus.GaugeValue,
				toFloat64("metricTrailIoWriteCount", trailout.IoWriteCount),
				[]string{trailout.TrailName, trailout.TrailPath, trailout.Hostname, pump.Process.Name}...)
			ch <- prometheus.MustNewConstMetric(collector.metricTrailIoWriteByte,
				prometheus.GaugeValue,
				toFloat64("metricTrailIoWriteByte", trailout.IoWriteBytes),
				[]string{trailout.TrailName, trailout.TrailPath, trailout.Hostname, pump.Process.Name}...)
			// -- RBA + SEQ
			ch <- prometheus.MustNewConstMetric(collector.metricTrailRba,
				prometheus.GaugeValue,
				toFloat64("metricTrailRba", trailout.TrailRba),
				[]string{trailout.TrailName, trailout.TrailPath, trailout.Hostname, pump.Process.Name}...)
			ch <- prometheus.MustNewConstMetric(collector.metricTrailSeq,
				prometheus.GaugeValue,
				toFloat64("metricTrailSeq", trailout.TrailSeq),
				[]string{trailout.TrailName, trailout.TrailPath, trailout.Hostname, pump.Process.Name}...)
			//========== metricTrailMaxBytes
			ch <- prometheus.MustNewConstMetric(collector.metricTrailMaxBytes,
				prometheus.GaugeValue,
				toFloat64("metricTrailMaxBytes", trailout.TrailMaxBytes),
				[]string{trailout.TrailName, trailout.TrailPath, trailout.Hostname, pump.Process.Name}...)

		}
	}

	// ===== PMSRVR     =======

	log.Debugf("performanceServer")
	log.Debugf("  - %s", performanceServer.Name)
	ch <- prometheus.MustNewConstMetric(collector.metricStatus,
		prometheus.GaugeValue,
		toFloat64("metricStatus", performanceServer.Process.Status),
		[]string{config.MgrHost, performanceServer.Process.Name, typeToString(performanceServer.Process.Type)}...)

	// ===== REPLICAT   =======
	log.Debugf("Replicat")
	for _, replicat := range *listOfReplicat {
		log.Debugf("  - %s", replicat.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricStatus,
			prometheus.GaugeValue,
			toFloat64("", replicat.Process.Status),
			[]string{config.MgrHost, replicat.Process.Name, typeToString(replicat.Process.Type)}...)
		ch <- prometheus.MustNewConstMetric(collector.metricLastOperationLag,
			prometheus.GaugeValue,
			toFloat64("", replicat.Process.PositionEr.LastOperationLag),
			replicat.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricLastOperationTs,
			prometheus.GaugeValue,
			toUnixTime(replicat.Process.PositionEr.LastOperationTs),
			replicat.Process.Name)
		ch <- prometheus.MustNewConstMetric(collector.metricLastCheckpointTs,
			prometheus.GaugeValue,
			toUnixTime(replicat.Process.PositionEr.LastCheckpointTs),
			replicat.Process.Name)

		for _, trailin := range replicat.Process.TrailInput {
			// -- Read
			ch <- prometheus.MustNewConstMetric(collector.metricTrailIoReadCount,
				prometheus.GaugeValue,
				toFloat64("metricTrailIoReadCount", trailin.IoReadCount),
				[]string{trailin.TrailName, trailin.TrailPath, config.MgrHost, replicat.Process.Name}...)
			ch <- prometheus.MustNewConstMetric(collector.metricTrailIoReadByte,
				prometheus.GaugeValue,
				toFloat64("metricTrailIoReadByte", trailin.IoReadBytes),
				[]string{trailin.TrailName, trailin.TrailPath, config.MgrHost, replicat.Process.Name}...)
			// -- RBA + SEQ
			ch <- prometheus.MustNewConstMetric(collector.metricTrailRba,
				prometheus.GaugeValue,
				toFloat64("metricTrailRba", trailin.TrailRba),
				[]string{trailin.TrailName, trailin.TrailPath, config.MgrHost, replicat.Process.Name}...)
			ch <- prometheus.MustNewConstMetric(collector.metricTrailSeq,
				prometheus.GaugeValue,
				toFloat64("metricTrailSeq", trailin.TrailSeq),
				[]string{trailin.TrailName, trailin.TrailPath, config.MgrHost, replicat.Process.Name}...)
		}
		// Dem so luong field trong Statistics sau do chuyen thanh Lable
		a := reflect.ValueOf(&replicat.Process.StatisticsReplicat).Elem()
		for i := 0; i < (a.NumField()); i++ {
			if a.Type().Field(i).Name != "Text" {
				ch <- prometheus.MustNewConstMetric(collector.metricStatistics,
					prometheus.GaugeValue,
					toFloat64("StatisticsReplicat."+a.Type().Field(i).Name, fmt.Sprintf("%s", a.Field(i).Interface())),
					[]string{config.MgrHost, replicat.Process.Name, a.Type().Field(i).Name}...)
			}
		}
	}

	log.Debugf("===== END =================================================")
}

// ------ Chuyen tu string trong object thanh float64 phu hop voi metric gauge
func toFloat64(name string, input string) float64 {
	metric, er := strconv.ParseFloat(input, 64)
	if er != nil {
		log.Errorf("\t  %s :", name)
		log.Errorf("\t    => Services.toFloat64. Noi dung dau vao (%s) khong phu hop", input)
		return 0
	}
	return metric
}

func getInputCheckPointValue(input string) float64 {
	if input == "" {
		log.Warnf("Service.Collector.getInputCheckPointValue(): empty input")
		return 0
	}
	index := strings.Index(input, "Timestamp: ")
	rfc3339t := strings.Replace(strings.TrimSpace(input[index+10:]), " ", "T", 1) + "Z"
	t, err := time.Parse(time.RFC3339, rfc3339t)
	if err != nil {
		log.Warnf("Service.Collector.getInputCheckPointValue(%s) error or not running yet.", input)
		return 0
	}
	ut := t.UnixNano() / int64(time.Millisecond)
	return float64(ut)
}

func toUnixTime(input string) float64 {
	if input == "" {
		log.Warnf("Service.Collector.toUnixTime(): empty input")
		return 0
	}
	rfc3339t := input + "Z"
	t, err := time.Parse(time.RFC3339, rfc3339t)
	if err != nil {
		log.Warnf("Service.Collector.toUnixTime(%s) error or not running yet.", input)
		return 0
	}
	ut := t.UnixNano() / int64(time.Millisecond)
	return float64(ut)
}

// ------ Chuyen tu string type trong object thanh cac string day du, de hieu
func typeToString(inputString string) string {
	if inputString == model.TYPE_PMSRVR {
		return "Performance_Metrics_Server"
	}
	if inputString == model.TYPE_MGR {
		return "Manager"
	}
	if inputString == model.TYPE_EXTRACT {
		return "Extract_Capture"
	}
	if inputString == model.TYPE_PUMP {
		return "Extract_Pump"
	}
	if inputString == model.TYPE_REPLICAT {
		return "Replicat_Delivery"
	}
	log.Errorf("Services.Collector.Status.Khong the chuyen type %s thanh string", inputString)
	return "Unknown"
}
