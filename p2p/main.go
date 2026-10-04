package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"slices"
	"time"
)

func main() {
	modo := flag.String("modo", "", "seed | peer")
	porta := flag.Int("porta", 0, "porta TCP deste nó (0 = qualquer; no seed o padrão é 6000)")
	host := flag.String("host", "127.0.0.1", "IP deste nó, como os outros vão enxergá-lo (modo peer)")
	seedAddr := flag.String("seed", "127.0.0.1:6000", "endereço host:porta do seed/tracker (modo peer)")
	arquivo := flag.String("arquivo", "arq.bin", "arquivo a compartilhar (modo seed)")
	tamPedaco := flag.Int64("pedaco", 256*1024, "tamanho do pedaço em bytes (modo seed)")
	esperados := flag.Int("esperados", 1, "quantos peers vão participar (modo seed)")
	banda := flag.Float64("banda", 0, "limite de upload deste nó em Mbit/s (0 = sem limite)")
	verificar := flag.Bool("verificar", false, "confere o SHA-256 do arquivo baixado (modo peer; use só em testes)")
	flag.Parse()

	var err error
	switch *modo {
	case "seed":
		err = rodarSeed(*arquivo, *tamPedaco, *esperados, *porta, *banda)
	case "peer":
		err = rodarPeer(*host, *porta, *seedAddr, *banda, *verificar)
	default:
		err = fmt.Errorf("use -modo seed ou -modo peer")
	}
	if err != nil {
		log.Fatal(err)
	}
}

func rodarSeed(arquivo string, tamPedaco int64, esperados, porta int, banda float64) error {
	if porta == 0 {
		porta = 6000
	}
	s, err := novoSeed(arquivo, tamPedaco, esperados, banda)
	if err != nil {
		return err
	}
	defer s.arq.Close()

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", porta))
	if err != nil {
		return err
	}
	defer ln.Close()
	log.Printf("seed na porta %d: %d MB em %d pedaços de %d KB, esperando %d peers (banda=%.0f Mbit/s)",
		porta, s.meta.Tamanho>>20, s.meta.NumPedacos, s.meta.TamPedaco>>10, esperados, banda)

	go s.servir(ln)
	<-s.fim
	time.Sleep(time.Second)

	imprimirResultados(s.resultados(), esperados)
	return nil
}

func rodarPeer(host string, porta int, seedAddr string, banda float64, verificar bool) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", porta))
	if err != nil {
		return err
	}
	defer ln.Close()
	porta = ln.Addr().(*net.TCPAddr).Port
	meu := fmtAddr(host, porta)

	// registra no tracker e recebe os metadados do arquivo
	resp, err := consultarTracker(seedAddr, ReqTracker{Tipo: "registrar", Addr: meu})
	if err != nil {
		return fmt.Errorf("registrando no tracker: %w", err)
	}
	p, err := novoPeer(resp.Meta, banda)
	if err != nil {
		return err
	}
	defer p.fechar()
	go p.servir(ln)

	// espera a largada (todos os peers esperados registrados)
	for !resp.Iniciar {
		time.Sleep(20 * time.Millisecond)
		if resp, err = consultarTracker(seedAddr, ReqTracker{Tipo: "consultar", Addr: meu}); err != nil {
			return err
		}
	}

	// baixa do seed e dos outros peers; o tempo conta da largada até o último pedaço
	t0 := time.Now()
	vizinhos := []string{seedAddr}
	for _, a := range resp.Peers {
		if a != meu {
			vizinhos = append(vizinhos, a)
		}
	}
	p.baixar(vizinhos)
	seg := time.Since(t0).Seconds()
	log.Printf("%s: download concluído em %.3fs", meu, seg)

	if _, err := consultarTracker(seedAddr, ReqTracker{Tipo: "pronto", Addr: meu, Seg: seg}); err != nil {
		return err
	}

	if verificar {
		ok, err := p.hashOK()
		switch {
		case err != nil:
			log.Printf("%s: erro ao verificar: %v", meu, err)
		case ok:
			log.Printf("%s: SHA-256 confere", meu)
		default:
			log.Printf("%s: ERRO: SHA-256 NÃO confere", meu)
		}
	}

	for {
		time.Sleep(100 * time.Millisecond)
		r, err := consultarTracker(seedAddr, ReqTracker{Tipo: "consultar", Addr: meu})
		if err != nil || r.Fim {
			return nil
		}
	}
}

func imprimirResultados(tempos []float64, esperados int) {
	if len(tempos) == 0 {
		log.Println("nenhum resultado")
		return
	}
	soma := 0.0
	for _, t := range tempos {
		soma += t
	}
	fmt.Printf("válidos=%d/%d min=%.3f med=%.3f max=%.3f\n",
		len(tempos), esperados, slices.Min(tempos), soma/float64(len(tempos)), slices.Max(tempos))
}
