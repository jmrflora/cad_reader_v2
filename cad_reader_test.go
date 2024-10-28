package cadreaderv2

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSomething(t *testing.T) {
	assert.True(t, true, "True is true!")
}

func TestCADS01(t *testing.T) {
	cs, err := GetAllCad01("internal/assets/CAD01.dbf")
	assert.NoError(t, err)
	// fmt.Printf("cs: %v\n", cs)
	fmt.Printf("cs[0]: %v\n", cs[0])
}

func TestMortos(t *testing.T) {
	ms, err := Mortos("internal/assets/MORTO01.DBF")
	assert.NoError(t, err)
	// fmt.Printf("ms: %v\n", ms)
	fmt.Printf("ms[0]: %v\n", ms[0])
}

func TestProcedure(t *testing.T) {
	_, err := procedure("internal/assets/CAD01.dbf", "internal/assets/CAD01_copia.dbf")
	assert.NoError(t, err)
}

func TestProcedureProcurarInsert(t *testing.T) {
	result := procedureProcurarInsert("internal/assets/CAD01.dbf", "internal/assets/CAD01_copia.dbf")
	assert.NotEmpty(t, result)
}

// "internal/assets/CAD01.DBF"

func TestProcurarCad01PorInscr(t *testing.T) {
	_, err := ProcurarCad01PorInscr("internal/assets/CAD01.DBF", "0")
	assert.Error(t, err)
	result, err := ProcurarCad01PorInscr("internal/assets/CAD01.DBF", "200257980")
	assert.NoError(t, err)
	fmt.Printf("result: %v\n", result)
}

func TestGetCad(t *testing.T) {
	cad, err := GetCad01Modelo("internal/assets/CAD01.DBF", "200257980")
	assert.NoError(t, err)
	fmt.Printf("cad: %v\n", cad)
	fmt.Printf("cad.Depro: %v\n", cad.Depro)
}
