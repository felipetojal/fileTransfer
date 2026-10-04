package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/felipetojal/fileTransfer/internal/limitador"
)

type Peer struct {
	meta Meta
	lim  *limitador.Limitador
	arq  *os.File

	mu        sync.RWMutex
	bits      []byte
	emVoo     []bool
	baixados  int
	concluido chan struct{}
}

func novoPeer(meta Meta, banda float64) (*Peer, error) {
	f, err := os.CreateTemp("", "p2p-*.part")
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(meta.Tamanho); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	return &Peer{
		meta:      meta,
		lim:       limitador.Novo(banda),
		arq:       f,
		bits:      make([]byte, (meta.NumPedacos+7)/8),
		emVoo:     make([]bool, meta.NumPedacos),
		concluido: make(chan struct{}),
	}, nil
}

func (p *Peer) fechar() {
	p.arq.Close()
	os.Remove(p.arq.Name())
}

func (p *Peer) tem(i int) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return temBit(p.bits, i)
}

// bitfield devolve uma cópia, porque servirPedacos lê o resultado sem travar
func (p *Peer) bitfield() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return slices.Clone(p.bits)
}

func (p *Peer) ler(i int, buf []byte) error {
	_, err := p.arq.ReadAt(buf, int64(i)*p.meta.TamPedaco)
	return err
}

// lado servidor: atende os outros peers

func (p *Peer) servir(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go p.atender(conn)
	}
}

func (p *Peer) atender(conn net.Conn) {
	hello := make([]byte, 1)
	if _, err := io.ReadFull(conn, hello); err != nil || hello[0] != helloPedacos {
		conn.Close()
		return
	}
	servirPedacos(conn, p, p.meta, p.lim)
}

// lado cliente: baixa dos vizinhos
func (p *Peer) baixar(vizinhos []string) {
	for _, addr := range vizinhos {
		go p.trabalhar(addr)
	}
	<-p.concluido
}

// trabalhar baixa de um vizinho: pega o bitfield dele, pede os pedaços que
func (p *Peer) trabalhar(addr string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("vizinho %s: %v", addr, err)
		return
	}
	defer conn.Close()
	if _, err := conn.Write([]byte{helloPedacos}); err != nil {
		return
	}
	r := bufio.NewReader(conn)
	buf := make([]byte, p.meta.TamPedaco)

	for !p.completo() {
		bf, err := pedirBitfield(conn, r)
		if err != nil {
			return
		}
		for {
			i := p.reservar(bf)
			if i < 0 {
				break // nada útil neste vizinho por enquanto
			}
			n, err := pedirPedaco(conn, r, i, buf)
			if err != nil {
				p.liberar(i)
				return
			}
			if int64(n) != p.meta.tamanhoDo(i) {
				p.liberar(i) // o vizinho não tinha o pedaço
				break
			}
			if err := p.guardar(i, buf[:n]); err != nil {
				log.Printf("gravando pedaço %d: %v", i, err)
				return
			}
		}
		select {
		case <-p.concluido:
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// reservar escolhe um pedaço que falta, que ninguém está baixando e que o
// vizinho tem. A busca começa num ponto aleatório: assim os peers pegam
// pedaços diferentes e depois trocam entre si (se todos pedissem na mesma
// ordem, ninguém teria nada para oferecer aos outros). Devolve -1 se não houver.
func (p *Peer) reservar(bfVizinho []byte) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := p.meta.NumPedacos
	ini := rand.IntN(n)
	for k := 0; k < n; k++ {
		i := (ini + k) % n
		if !temBit(p.bits, i) && !p.emVoo[i] && temBit(bfVizinho, i) {
			p.emVoo[i] = true
			return i
		}
	}
	return -1
}

func (p *Peer) liberar(i int) {
	p.mu.Lock()
	p.emVoo[i] = false
	p.mu.Unlock()
}

func (p *Peer) guardar(i int, dados []byte) error {
	if _, err := p.arq.WriteAt(dados, int64(i)*p.meta.TamPedaco); err != nil {
		p.liberar(i)
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	setBit(p.bits, i)
	p.emVoo[i] = false
	p.baixados++
	if p.baixados == p.meta.NumPedacos {
		close(p.concluido)
	}
	return nil
}

func (p *Peer) completo() bool {
	select {
	case <-p.concluido:
		return true
	default:
		return false
	}
}

// hashOK confere o SHA-256 do arquivo baixado com o do seed (flag -verificar)
func (p *Peer) hashOK() (bool, error) {
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(p.arq, 0, p.meta.Tamanho)); err != nil {
		return false, err
	}
	return hex.EncodeToString(h.Sum(nil)) == p.meta.SHA256, nil
}
