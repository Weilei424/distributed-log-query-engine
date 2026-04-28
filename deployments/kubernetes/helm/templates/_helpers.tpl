{{- define "logengine.raftPeers" -}}
{{- $replicas := .Values.coordinator.replicas | int -}}
{{- $peers := list -}}
{{- range $i := until $replicas -}}
{{- $name := printf "coordinator-%d" $i -}}
{{- $addr := printf "%s.coordinator.logengine.svc.cluster.local:7000" $name -}}
{{- $peers = append $peers (printf "%s=%s" $name $addr) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}

{{- define "logengine.coordinatorAddrs" -}}
{{- $replicas := .Values.coordinator.replicas | int -}}
{{- $port := .Values.coordinator.grpcPort | int -}}
{{- $addrs := list -}}
{{- range $i := until $replicas -}}
{{- $addr := printf "coordinator-%d.coordinator.logengine.svc.cluster.local:%d" $i $port -}}
{{- $addrs = append $addrs $addr -}}
{{- end -}}
{{- join "," $addrs -}}
{{- end -}}
