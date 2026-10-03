// Protocolo de rede do P2P: mensagens do tracker (JSON) e da troca de pedaços (binário).
package main

import (
	"encoding/json"
	"net"
	"time"
)

// Primeiro byte de toda conexão: diz se é conversa com o tracker ou troca de pedaços.
const (
	helloTracker byte = 'T'
	helloPedacos byte = 'P'
)

// Pedidos na sessão de pedaços: [1 byte tipo][4 bytes índice].
// Respostas: [4 bytes tamanho][dados]. Tamanho 0 = "não tenho esse pedaço".
const (
	reqBitfield byte = 1
	reqPedaco   byte = 2
)

// Meta descreve o arquivo compartilhado (o "torrent").
type Meta struct {
	Tamanho    int64  `json:"tamanho"`
	TamPedaco  int64  `json:"tam_pedaco"`
	NumPedacos int    `json:"num_pedacos"`
	SHA256     string `json:"sha256"`
}

func novaMeta(tamanho, tamPedaco int64) Meta {
	n := int((tamanho + tamPedaco - 1) / tamPedaco)
	return Meta{Tamanho: tamanho, TamPedaco: tamPedaco, NumPedacos: n}
}

// tamanhoDo devolve o tamanho do pedaço i (o último pode ser menor).
func (m Meta) tamanhoDo(i int) int64 {
	ini := int64(i) * m.TamPedaco
	fim := ini + m.TamPedaco
	if fim > m.Tamanho {
		fim = m.Tamanho
	}
	return fim - ini
}

// tracker

// ReqTracker: "registrar" (entrar na rede), "consultar" (pegar lista atualizada)
// e "pronto" (avisar que terminou o download, com o tempo em segundos).
type ReqTracker struct {
	Tipo string  `json:"tipo"`
	Addr string  `json:"addr"`
	Seg  float64 `json:"seg,omitempty"`
}

type RespTracker struct {
	Meta    Meta     `json:"meta"`
	Peers   []string `json:"peers"`
	Iniciar bool     `json:"iniciar"` // todos os peers esperados já se registraram
	Fim     bool     `json:"fim"`     // todos os peers já terminaram
}

func consultarTracker(tracker string, req ReqTracker) (RespTracker, error) {
	var resp RespTracker
	conn, err := net.DialTimeout("tcp", tracker, 3*time.Second)
	if err != nil {
		return resp, err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte{helloTracker}); err != nil {
		return resp, err
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return resp, err
	}
	err = json.NewDecoder(conn).Decode(&resp)
	return resp, err
}

// bitfield
