// Seed: tem o arquivo completo, serve pedaços e também faz o papel de tracker
// (lista de peers, largada sincronizada e coleta dos tempos).
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"slices"
	"sync"
)

type Seed struct {
	meta      Meta
	arq       *os.File
	lim       *Limitador
	esperados int // quantos peers precisam se registrar para a largada

	mu        sync.Mutex
	peers     []string
	tempos    map[string]float64
	iniciou   bool
	acabou    bool
	fim       chan struct{} // fechado quando todos os peers terminaram
	bitsTodos []byte
}

func novoSeed(caminho string, tamPedaco int64, esperados int, banda float64) (*Seed, error) {
	f, err := os.Open(caminho)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	meta := novaMeta(info.Size(), tamPedaco)

	// hash do arquivo inteiro: os peers podem conferir a integridade no final (-verificar)
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	meta.SHA256 = hex.EncodeToString(h.Sum(nil))

	bits := make([]byte, (meta.NumPedacos+7)/8)
	for i := 0; i < meta.NumPedacos; i++ {
		setBit(bits, i)
	}
	return &Seed{
		meta: meta, arq: f, lim: novoLimitador(banda), esperados: esperados,
		tempos: map[string]float64{}, fim: make(chan struct{}), bitsTodos: bits,
	}, nil
}

// Armazem: o seed tem todos os pedaços.
func (s *Seed) tem(i int) bool   { return true }
func (s *Seed) bitfield() []byte { return s.bitsTodos }
func (s *Seed) ler(i int, buf []byte) error {
	_, err := s.arq.ReadAt(buf, int64(i)*s.meta.TamPedaco)
	return err
}

func (s *Seed) servir(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go s.atender(conn)
	}
}

func (s *Seed) atender(conn net.Conn) {
	hello := make([]byte, 1)
	if _, err := io.ReadFull(conn, hello); err != nil {
		conn.Close()
		return
	}
	switch hello[0] {
	case helloPedacos:
		servirPedacos(conn, s, s.meta, s.lim)
	case helloTracker:
		defer conn.Close()
		var req ReqTracker
		if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&req); err != nil {
			return
		}
		resp := s.tracker(req)
		json.NewEncoder(conn).Encode(resp)
	default:
		conn.Close()
	}
}

func (s *Seed) tracker(req ReqTracker) RespTracker {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch req.Tipo {
	case "registrar":
		if !slices.Contains(s.peers, req.Addr) {
			s.peers = append(s.peers, req.Addr)
			log.Printf("[tracker] peer registrado: %s (%d/%d)", req.Addr, len(s.peers), s.esperados)
		}
		if len(s.peers) >= s.esperados && !s.iniciou {
			s.iniciou = true
			log.Printf("[tracker] todos registrados, largada!")
		}
	case "pronto":
		if _, ok := s.tempos[req.Addr]; !ok {
			s.tempos[req.Addr] = req.Seg
			log.Printf("[tracker] %s terminou em %.3fs (%d/%d)", req.Addr, req.Seg, len(s.tempos), s.esperados)
		}
		if len(s.tempos) >= s.esperados && !s.acabou {
			s.acabou = true
			close(s.fim)
		}
	}
	return RespTracker{
		Meta: s.meta, Peers: slices.Clone(s.peers),
		Iniciar: s.iniciou, Fim: s.acabou,
	}
}

func (s *Seed) resultados() []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var r []float64
	for _, t := range s.tempos {
		r = append(r, t)
	}
	return r
}
