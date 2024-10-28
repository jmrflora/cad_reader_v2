package cadreaderv2

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Valentin-Kaiser/go-dbase/dbase"
)

type Cad02Modelo struct {
	Depro        string
	Nome         string
	DTNASC       string
	Sexo         string
	Documento    string
	Inscricao    string
	DataInclusao string
	Situacao     string
}

type Morto01Modelo struct {
	INSCR string
	DTEXC string
	CATEG string
}

type Cad01Modelo struct {
	Depro      string
	Nome       string
	Empresa    string //"n"
	DTNASC     string
	ESTCivil   string
	DTADM      string
	ENDRUANUM  string
	Complement string
	Bairro     string
	Cidade     string
	UF         string
	Telef      string
	CEP        string
	Lotacao    string
	INSCR      string //"unique"
	DTINS      string
	CATEG      string
	CODBCO     string
	CCRR       string
	CAPE       string //"n"
	NVIA       string //"n"
	NPREV      string
	TPAG       string
	DATEM      string //"n"
	CPF        string
	AGBB       string //"n"
	AGDV       string //"n"
	CCBB       string //"n"
	CCDV       string //"n"
	Celular    string
	CONV       string
	Email      string
}

type Mudanca struct {
	LinhaAntiga     *dbase.Row
	linhaNova       *dbase.Row
	CamposAlterados []*dbase.Field
}

type Resultado struct {
	mu   sync.Mutex
	mapa map[string]*Mudanca
}

func (r *Resultado) set(mud *Mudanca, valorInscr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mapa[valorInscr] = mud
}

const col int = 32

func GetCad02ModeloPorLinha(linha *dbase.Row) Cad02Modelo {
	campos := linha.Fields()
	return Cad02Modelo{
		Depro:        fmt.Sprintf("%v", campos[0].GetValue()),
		Nome:         fmt.Sprintf("%v", campos[1].GetValue()),
		DTNASC:       fmt.Sprintf("%v", campos[2].GetValue()),
		Sexo:         fmt.Sprintf("%v", campos[3].GetValue()),
		Situacao:     fmt.Sprintf("%v", campos[4].GetValue()),
		Documento:    fmt.Sprintf("%v", campos[5].GetValue()),
		Inscricao:    fmt.Sprintf("%v", campos[6].GetValue()),
		DataInclusao: fmt.Sprintf("%v", campos[7].GetValue()),
	}
}

func GetMorto01ModeloPorLinha(linha *dbase.Row) Morto01Modelo {
	campos := linha.Fields()
	return Morto01Modelo{
		INSCR: fmt.Sprintf("%v", campos[13].GetValue()),
		DTEXC: fmt.Sprintf("%v", campos[18].GetValue()),
		CATEG: fmt.Sprintf("%v", campos[15].GetValue()),
	}
}

func GetCAd01ModeloPorLinha(linha *dbase.Row) Cad01Modelo {
	campos := linha.Fields()
	return Cad01Modelo{
		Depro:      fmt.Sprintf("%v", campos[0].GetValue()),
		Nome:       fmt.Sprintf("%v", campos[1].GetValue()),
		Empresa:    fmt.Sprintf("%v", campos[2].GetValue()),
		DTNASC:     fmt.Sprintf("%v", campos[3].GetValue()),
		ESTCivil:   fmt.Sprintf("%v", campos[4].GetValue()),
		DTADM:      fmt.Sprintf("%v", campos[5].GetValue()),
		ENDRUANUM:  fmt.Sprintf("%v", campos[6].GetValue()),
		Complement: fmt.Sprintf("%v", campos[7].GetValue()),
		Bairro:     fmt.Sprintf("%v", campos[8].GetValue()),
		Cidade:     fmt.Sprintf("%v", campos[9].GetValue()),
		UF:         fmt.Sprintf("%v", campos[10].GetValue()),
		Telef:      fmt.Sprintf("%v", campos[11].GetValue()),
		CEP:        fmt.Sprintf("%v", campos[12].GetValue()),
		Lotacao:    fmt.Sprintf("%v", campos[13].GetValue()),
		INSCR:      fmt.Sprintf("%v", campos[14].GetValue()),
		DTINS:      fmt.Sprintf("%v", campos[15].GetValue()),
		CATEG:      fmt.Sprintf("%v", campos[16].GetValue()),
		CODBCO:     fmt.Sprintf("%v", campos[17].GetValue()),
		CCRR:       fmt.Sprintf("%v", campos[18].GetValue()),
		CAPE:       fmt.Sprintf("%v", campos[19].GetValue()),
		NVIA:       fmt.Sprintf("%v", campos[20].GetValue()),
		NPREV:      fmt.Sprintf("%v", campos[21].GetValue()),
		TPAG:       fmt.Sprintf("%v", campos[22].GetValue()),
		DATEM:      fmt.Sprintf("%v", campos[23].GetValue()),
		CPF:        fmt.Sprintf("%v", campos[24].GetValue()),
		AGBB:       fmt.Sprintf("%v", campos[25].GetValue()),
		AGDV:       fmt.Sprintf("%v", campos[26].GetValue()),
		CCBB:       fmt.Sprintf("%v", campos[27].GetValue()),
		CCDV:       fmt.Sprintf("%v", campos[28].GetValue()),
		Celular:    fmt.Sprintf("%v", campos[29].GetValue()),
		CONV:       fmt.Sprintf("%v", campos[30].GetValue()),
		Email:      fmt.Sprintf("%v", campos[31].GetValue()),
	}
}

func GetCad02modelo(Filename string, inscr string) (Cad02Modelo, error) {
	pos, err := ProcurarCad02PorInscr(Filename, inscr)
	if err != nil {
		return Cad02Modelo{}, err
	}
	tabelaCad02, err := dbase.OpenTable(&dbase.Config{
		Filename:   Filename,
		TrimSpaces: true,
		Untested:   true,
	})
	if err != nil {
		return Cad02Modelo{}, nil
	}
	defer tabelaCad02.Close()
	tabelaCad02.GoTo(uint32(pos))
	row, err := tabelaCad02.Row()
	if err != nil {
		return Cad02Modelo{}, err
	}

	return GetCad02ModeloPorLinha(row), nil
}

func GetCad01Modelo(Filename string, inscr string) (Cad01Modelo, error) {
	pos, err := ProcurarCad01PorInscr(Filename, inscr)
	if err != nil {
		return Cad01Modelo{}, err
	}
	tableCad01, err := dbase.OpenTable(&dbase.Config{
		Filename:   Filename,
		TrimSpaces: true,
		Untested:   true,
	})
	if err != nil {
		return Cad01Modelo{}, nil
	}
	defer tableCad01.Close()

	tableCad01.GoTo(uint32(pos))
	linha, err := tableCad01.Row()
	if err != nil {
		return Cad01Modelo{}, err
	}
	campos := linha.Fields()
	return Cad01Modelo{
		Depro:      fmt.Sprintf("%v", campos[0].GetValue()),
		Nome:       fmt.Sprintf("%v", campos[1].GetValue()),
		Empresa:    fmt.Sprintf("%v", campos[2].GetValue()),
		DTNASC:     fmt.Sprintf("%v", campos[3].GetValue()),
		ESTCivil:   fmt.Sprintf("%v", campos[4].GetValue()),
		DTADM:      fmt.Sprintf("%v", campos[5].GetValue()),
		ENDRUANUM:  fmt.Sprintf("%v", campos[6].GetValue()),
		Complement: fmt.Sprintf("%v", campos[7].GetValue()),
		Bairro:     fmt.Sprintf("%v", campos[8].GetValue()),
		Cidade:     fmt.Sprintf("%v", campos[9].GetValue()),
		UF:         fmt.Sprintf("%v", campos[10].GetValue()),
		Telef:      fmt.Sprintf("%v", campos[11].GetValue()),
		CEP:        fmt.Sprintf("%v", campos[12].GetValue()),
		Lotacao:    fmt.Sprintf("%v", campos[13].GetValue()),
		INSCR:      fmt.Sprintf("%v", campos[14].GetValue()),
		DTINS:      fmt.Sprintf("%v", campos[15].GetValue()),
		CATEG:      fmt.Sprintf("%v", campos[16].GetValue()),
		CODBCO:     fmt.Sprintf("%v", campos[17].GetValue()),
		CCRR:       fmt.Sprintf("%v", campos[18].GetValue()),
		CAPE:       fmt.Sprintf("%v", campos[19].GetValue()),
		NVIA:       fmt.Sprintf("%v", campos[20].GetValue()),
		NPREV:      fmt.Sprintf("%v", campos[21].GetValue()),
		TPAG:       fmt.Sprintf("%v", campos[22].GetValue()),
		DATEM:      fmt.Sprintf("%v", campos[23].GetValue()),
		CPF:        fmt.Sprintf("%v", campos[24].GetValue()),
		AGBB:       fmt.Sprintf("%v", campos[25].GetValue()),
		AGDV:       fmt.Sprintf("%v", campos[26].GetValue()),
		CCBB:       fmt.Sprintf("%v", campos[27].GetValue()),
		CCDV:       fmt.Sprintf("%v", campos[28].GetValue()),
		Celular:    fmt.Sprintf("%v", campos[29].GetValue()),
		CONV:       fmt.Sprintf("%v", campos[30].GetValue()),
		Email:      fmt.Sprintf("%v", campos[31].GetValue()),
	}, nil
}

func ProcurarCad02PorInscr(filename string, inscr string) (int, error) {
	tabelaCad02, err := dbase.OpenTable(&dbase.Config{
		Filename:   filename,
		TrimSpaces: true,
		Untested:   true,
	})
	if err != nil {
		return -1, err
	}
	defer tabelaCad02.Close()
	count := tabelaCad02.RowsCount()
	var i uint32

	for i = 0; i < count; i++ {
		row, err := tabelaCad02.Next()
		if err != nil {
			return -1, err
		}
		if row.Deleted {
			fmt.Printf("Deleted row at position: %v \n", row.Position)
			continue
		}
		field_inscr := row.Field(6)

		if field_inscr == nil {
			return 0, errors.New("field not found")
		}
		if field_inscr.GetValue() == inscr {
			return int(row.Position), nil
		}
	}
	return 0, errors.New("none found")
}

func ProcurarCad01PorInscr(filename string, inscr string) (int, error) {
	tableCad01, err := dbase.OpenTable(&dbase.Config{
		Filename:   filename,
		TrimSpaces: true,
		Untested:   true,
	})
	if err != nil {
		panic(err)
	}
	defer tableCad01.Close()

	count := tableCad01.RowsCount()
	var i uint32
	for i = 0; i < count; i++ {
		row, err := tableCad01.Next()
		if err != nil {
			panic(err)
		}
		if row.Deleted {
			fmt.Printf("Deleted row at position: %v \n", row.Position)
			continue
		}
		// field_nome := row.Field(1)
		// if field_nome == nil {
		// 	return 0, errors.New("field not found")
		// }
		// if field_nome.GetValue() == "CARLOS DIAS DE MELO" {
		// 	println("achei")
		// }
		field_inscr := row.Field(14)
		if field_inscr == nil {
			return 0, errors.New("field not found")
		}
		if field_inscr.GetValue() == inscr {
			return int(row.Position), nil
		}
	}
	return 0, errors.New("none found")
}

func GetAllCad01(filename string) ([]Cad01Modelo, error) {
	tabela, err := dbase.OpenTable(&dbase.Config{
		Filename:   filename,
		TrimSpaces: true,
		Untested:   true,
	})
	if err != nil {
		panic(err)
	}
	defer tabela.Close()

	linhas, err := tabela.Rows(false, true)
	if err != nil {
		panic(err)
	}
	resp := []Cad01Modelo{}
	for _, linha := range linhas {
		c := GetCAd01ModeloPorLinha(linha)
		resp = append(resp, c)
	}
	return resp, nil
}

func Mortos(filename string) ([]Morto01Modelo, error) {
	tabela, err := dbase.OpenTable(&dbase.Config{
		Filename:   filename,
		TrimSpaces: true,
		Untested:   true,
	})
	if err != nil {
		panic(err)
	}
	defer tabela.Close()

	linhas, err := tabela.Rows(false, true)
	if err != nil {
		panic(err)
	}
	resp := []Morto01Modelo{}
	for _, linha := range linhas {
		m := GetMorto01ModeloPorLinha(linha)
		resp = append(resp, m)
	}
	return resp, nil
}

func procedure(nomeArquivoAntigo string, nomeArquivoNovo string) (map[string]*Mudanca, error) {
	tabelaCadAntigo, err := dbase.OpenTable(&dbase.Config{
		Filename:   nomeArquivoAntigo,
		TrimSpaces: true,
		Untested:   true,
	})
	if err != nil {
		return nil, errors.New("aaaaa")
	}
	defer tabelaCadAntigo.Close()

	tabelaCadNovo, err := dbase.OpenTable(&dbase.Config{
		Filename:   nomeArquivoNovo,
		TrimSpaces: true,
		Untested:   true,
	})
	if err != nil {
		panic("erro na abertura do arquivo")
	}
	defer tabelaCadNovo.Close()

	mapa := make(map[string]*Mudanca)

	linhasCadAntigo, err := tabelaCadAntigo.Rows(false, true)
	if err != nil {
		panic("o oh so over")
	}
	linhasCadNovo, err := tabelaCadNovo.Rows(false, true)
	if err != nil {
		panic("o oh so over")
	}

	for _, linha := range linhasCadAntigo {

		campoInscricao := linha.Field(14)
		if campoInscricao == nil {
			return nil, errors.New("falha ao obter campo de inscrição")
		}

		valorInscr := campoInscricao.GetValue().(string)
		// debug
		// println(valorInscr)

		linhaNova, err := procurarLinhaPorInscr(linhasCadNovo, valorInscr)
		// significa que foi deletado uma inscrição na tabela nova
		if err != nil {
			// debug
			fmt.Println(err.Error())

			mapa[valorInscr] = new(Mudanca)
			mapa[valorInscr].linhaNova = nil
			mapa[valorInscr].LinhaAntiga = linha
			mapa[valorInscr].CamposAlterados = nil
			continue
		}

		result := compLinhaCad(linha, linhaNova)

		// nenhuma mudança
		if len(result) < 1 {
			continue
		}

		mapa[valorInscr] = new(Mudanca)
		mapa[valorInscr].linhaNova = linhaNova
		mapa[valorInscr].LinhaAntiga = linha
		mapa[valorInscr].CamposAlterados = result
	}

	return mapa, nil
}

func procurarLinhaPorInscr(linhas []*dbase.Row, inscr string) (*dbase.Row, error) {
	for _, linha := range linhas {
		// fmt.Printf("linha nova i: %d\n", i)
		campoInscricao := linha.Field(14)
		if campoInscricao == nil {
			panic("erro ao obter campo inscr")
		}

		// debug
		// println(campoInscricao.GetValue().(string))

		if campoInscricao.GetValue().(string) == inscr {
			return linha, nil
		}
	}

	return nil, errors.New("linha não encontrada")
}

func compLinhaCad(linhaAntiga *dbase.Row, linhaNova *dbase.Row) []*dbase.Field {
	var campos []*dbase.Field

	// debug
	// if linhaAntiga.Field(1).GetValue() == "CARLOS DIAS DE MELO" {
	// 	println("olá")
	// 	println(linhaNova.Field(1).GetValue().(string))
	// }

	for i := 0; i < col; i++ {
		campoLinhaAntiga := linhaAntiga.Field(i)
		campoLinhaNova := linhaNova.Field(i)
		// debug
		// println(campoLinhaAntiga.GetValue())
		// println(campoLinhaNova.GetValue())
		// valorLinhaAntiga := campoLinhaAntiga.GetValue()
		// valorLinhaNova := campoLinhaNova.GetValue()

		// switch v := valorLinhaAntiga.(type) {
		// case time.Time:
		// 	if campoLinhaAntiga.GetValue().(time.Time) != campoLinhaNova.GetValue().(time.Time) {
		// 		campos = append(campos, campoLinhaNova)
		// 	}
		// case string:
		// 	fmt.Printf("v is of type %T\n", v)
		// case int64:
		// 	fmt.Printf("v is of type %T\n", v)
		// default:
		// 	fmt.Printf("v is of unknown type\n")
		// }

		// debug

		// valorCampoLinhaAntiga := campoLinhaAntiga.GetValue()
		// valorLinhaCampoNova := campoLinhaNova.GetValue()

		// switch valorCampoLinhaAntiga.(type) {
		// case string:
		// 	fmt.Printf("\n\n %s -- %s\n", valorCampoLinhaAntiga, valorLinhaCampoNova)
		// case time.Time:
		// 	fmt.Printf("\n\n %04d-%02d-%02d -- %04d-%02d-%02d\n",
		// 		valorCampoLinhaAntiga.(time.Time).Year(),
		// 		valorCampoLinhaAntiga.(time.Time).Month(),
		// 		valorCampoLinhaAntiga.(time.Time).Day(),
		// 		valorLinhaCampoNova.(time.Time).Year(), valorLinhaCampoNova.(time.Time).Month(), valorLinhaCampoNova.(time.Time).Day())
		// }

		if campoLinhaAntiga.GetValue() != campoLinhaNova.GetValue() {
			// debug
			// println("achei mudança")
			// println(campoLinhaAntiga.GetValue().(string))
			// println(campoLinhaNova.GetValue().(string))

			campos = append(campos, campoLinhaNova)
		}
	}
	return campos
}

func testeTipos(linha *dbase.Row) {
	for i := 0; i < col; i++ {
		campolinha := linha.Field(i)
		value := campolinha.GetValue()
		fmt.Printf("tipo: %T\n", value)
	}
}

func procedureProcurarInsert(nomeArquivoAntigo string, nomeArquivoNovo string) []string {
	tabelaCadNovo, err := dbase.OpenTable(&dbase.Config{
		Filename:   nomeArquivoNovo,
		TrimSpaces: true,
		Untested:   true,
	})
	if err != nil {
		panic("erro na abertura do arquivo")
	}
	defer tabelaCadNovo.Close()

	tabelaCadAntigo, err := dbase.OpenTable(&dbase.Config{
		Filename:   nomeArquivoAntigo,
		TrimSpaces: true,
		Untested:   true,
	})
	if err != nil {
		panic("erro na abertura do arquivo")
	}
	defer tabelaCadAntigo.Close()

	linhasCadNovo, err := tabelaCadNovo.Rows(false, true)
	if err != nil {
		panic("o oh so over")
	}

	linhasCadAntigo, err := tabelaCadAntigo.Rows(false, true)
	if err != nil {
		panic("o oh so over")
	}

	var resposta []string

	for _, linhaNova := range linhasCadNovo {
		campoInscricao := linhaNova.Field(14)
		if campoInscricao == nil {
			panic("campo inscri == nil")
		}

		valorInscr := campoInscricao.GetValue().(string)

		_, err := procurarLinhaPorInscr(linhasCadAntigo, valorInscr)
		// significa que houve inserção no cad novo
		if err != nil {
			// debug
			// fmt.Println(err.Error())
			resposta = append(resposta, valorInscr)
		}
	}
	return resposta
}
