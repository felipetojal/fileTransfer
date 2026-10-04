package limitador

import (
	"io"
	"sync"
	"time"
)

// tamanho máximo de cada escrita limitada
const tamBloco = 32 * 1024

// Limitador controla a taxa de envio de um nó
type Limitador struct {
	mu   sync.Mutex
	taxa float64
	prox time.Time
}

// cria um limitador de mbps
func Novo(mbps float64) *Limitador {
	if mbps <= 0 {
		return nil
	}
	return &Limitador{taxa: mbps * 1e6 / 8}
}

func (l *Limitador) Esperar(n int) {
	if l == nil {
		return
	}
	l.mu.Lock()
	agora := time.Now()
	if l.prox.Before(agora) {
		l.prox = agora
	}
	l.prox = l.prox.Add(time.Duration(float64(n) / l.taxa * float64(time.Second)))
	d := l.prox.Sub(agora)
	l.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
}

// escreve dados em w, em blocos, respeitando o limite de l
func Escrever(w io.Writer, l *Limitador, dados []byte) error {
	for len(dados) > 0 {
		n := min(tamBloco, len(dados))
		l.Esperar(n)
		if _, err := w.Write(dados[:n]); err != nil {
			return err
		}
		dados = dados[n:]
	}
	return nil
}

// devolve um io.Writer que aplica o limite a cada Write
func (l *Limitador) Escritor(w io.Writer) io.Writer {
	return escritor{w: w, l: l}
}

type escritor struct {
	w io.Writer
	l *Limitador
}

func (e escritor) Write(p []byte) (int, error) {
	if err := Escrever(e.w, e.l, p); err != nil {
		return 0, err
	}
	return len(p), nil
}
