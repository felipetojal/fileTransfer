package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/felipetojal/fileTransfer/internal/limitador"
)

var (
	arquivo string
	// entre os clientes atendidos ao mesmo tempo, nil = sem limite.
	lim *limitador.Limitador
)

func main() {
	modo := flag.String("modo", "seq", "seq | thread | pool")
	porta := flag.Int("porta", 5000, "porta TCP")
	flag.StringVar(&arquivo, "arquivo", "arq.bin", "arquivo a ser enviado")
	n := flag.Int("n", 4, "numero maximo de clientes simultaneos (modo pool)")
	banda := flag.Float64("banda", 0, "limite de upload do servidor em Mbit/s (0 = sem limite)")
	flag.Parse()

	if _, err := os.Stat(arquivo); err != nil {
		log.Fatalf("arquivo invalido: %v", err)
	}
	lim = limitador.Novo(*banda)

	// listener para a conexão
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *porta))
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	log.Printf("servidor [%s] na porta %d (arquivo=%s, banda=%.0f Mbit/s)", *modo, *porta, arquivo, *banda)

	switch *modo {
	case "seq":
		// cliente por vez: atende na própria goroutine principal
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Println("accept:", err)
				continue
			}
			handle(conn)
		}

	case "thread":
		// todos de uma vez: 1 goroutine por conexão
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Println("accept:", err)
				continue
			}
			go handle(conn)
		}

	case "pool":
		// no máximo N por vez: N workers consumindo uma fila (canal com buffer)
		fila := make(chan net.Conn, 1000)
		for i := 0; i < *n; i++ {
			go func() {
				for c := range fila {
					handle(c)
				}
			}()
		}
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Println("accept:", err)
				continue
			}
			fila <- conn
		}

	default:
		log.Fatalf("modo invalido: %s", *modo)
	}
}

// só o conteúdo passa pelo limitador
func handle(conn net.Conn) {
	defer conn.Close()

	f, err := os.Open(arquivo)
	if err != nil {
		log.Println("open:", err)
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		log.Println("stat:", err)
		return
	}

	if err := binary.Write(conn, binary.BigEndian, info.Size()); err != nil {
		log.Println("cabecalho:", err)
		return
	}

	var dst io.Writer = conn
	if lim != nil {
		dst = lim.Escritor(conn)
	}
	if _, err := io.Copy(dst, f); err != nil {
		log.Println("envio:", err)
	}
}
