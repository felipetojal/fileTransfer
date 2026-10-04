// Protocolo de rede do P2P: mensagens do tracker (JSON) e da troca de pedaços (binário).
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/felipetojal/fileTransfer/internal/limitador"
	"io"
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

func temBit(bf []byte, i int) bool {
	return i/8 < len(bf) && bf[i/8]&(1<<(uint(i)%8)) != 0
}

func setBit(bf []byte, i int) { bf[i/8] |= 1 << (uint(i) % 8) }

//lado servidor da troca de pedaços (usado pelo seed e pelos peers)

// Armazem é quem guarda os pedaços: o seed (arquivo completo) ou um peer (arquivo parcial).
type Armazem interface {
	tem(i int) bool
	bitfield() []byte
	ler(i int, buf []byte) error
}

// servirPedacos atende uma conexão 'P': responde pedidos de bitfield e de pedaços.
// Só o envio de pedaços passa pelo limitador de banda (o bitfield tem poucos bytes).
func servirPedacos(conn net.Conn, a Armazem, meta Meta, lim *limitador.Limitador) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	hdr := make([]byte, 5)
	tam := make([]byte, 4)
	buf := make([]byte, meta.TamPedaco)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return
		}
		switch hdr[0] {
		case reqBitfield:
			bf := a.bitfield()
			binary.BigEndian.PutUint32(tam, uint32(len(bf)))
			if _, err := conn.Write(append(tam, bf...)); err != nil {
				return
			}
		case reqPedaco:
			i := int(binary.BigEndian.Uint32(hdr[1:]))
			if i < 0 || i >= meta.NumPedacos || !a.tem(i) {
				binary.BigEndian.PutUint32(tam, 0)
				if _, err := conn.Write(tam); err != nil {
					return
				}
				continue
			}
			n := meta.tamanhoDo(i)
			if err := a.ler(i, buf[:n]); err != nil {
				return
			}
			binary.BigEndian.PutUint32(tam, uint32(n))
			if _, err := conn.Write(tam); err != nil {
				return
			}
			if err := limitador.Escrever(conn, lim, buf[:n]); err != nil {
				return
			}
		default:
			return
		}
	}
}

//  lado cliente da troca de pedaços

func pedirBitfield(conn net.Conn, r *bufio.Reader) ([]byte, error) {
	if _, err := conn.Write([]byte{reqBitfield, 0, 0, 0, 0}); err != nil {
		return nil, err
	}
	n, err := lerTamanho(r)
	if err != nil {
		return nil, err
	}
	bf := make([]byte, n)
	_, err = io.ReadFull(r, bf)
	return bf, err
}

// pedirPedaco devolve quantos bytes vieram (0 = o vizinho não tinha o pedaço).
func pedirPedaco(conn net.Conn, r *bufio.Reader, i int, buf []byte) (int, error) {
	req := []byte{reqPedaco, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(req[1:], uint32(i))
	if _, err := conn.Write(req); err != nil {
		return 0, err
	}
	n, err := lerTamanho(r)
	if err != nil {
		return 0, err
	}
	if n > len(buf) {
		return 0, errors.New("pedaço maior que o esperado")
	}
	_, err = io.ReadFull(r, buf[:n])
	return n, err
}

func lerTamanho(r *bufio.Reader) (int, error) {
	var tam [4]byte
	if _, err := io.ReadFull(r, tam[:]); err != nil {
		return 0, err
	}
	return int(binary.BigEndian.Uint32(tam[:])), nil
}

func fmtAddr(host string, porta int) string { return fmt.Sprintf("%s:%d", host, porta) }
