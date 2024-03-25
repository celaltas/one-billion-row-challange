package main

import (
	"reflect"
	"testing"
)

func Test_processRow(t *testing.T) {
	type args struct {
		line string
	}
	tests := []struct {
		name    string
		args    args
		want    Record
		wantErr bool
	}{
		{
			name:    "valid line",
			args:    args{line: "Adana;23.4"},
			want:    Record{city: "Adana", temperature: 23.4},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := processLine(tt.args.line)
			if (err != nil) != tt.wantErr {
				t.Errorf("processRow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("processRow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStats_AddTemperature(t *testing.T) {
	type fields struct {
		min   float32
		max   float32
		sum   float32
		count float32
	}
	type args struct {
		temp float32
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Stats
	}{
		{
			name: "test 1",
			fields: fields{
				min:   1,
				max:   20,
				sum:   5,
				count: 4,
			},
			args: args{
				temp: 23,
			},
			want: &Stats{
				min:   1,
				max:   23,
				sum:   28,
				count: 5,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stats{
				min:   tt.fields.min,
				max:   tt.fields.max,
				sum:   tt.fields.sum,
				count: tt.fields.count,
			}
			got := s.AddTemperature(tt.args.temp)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Stats.AddTemperature() = %v, want %v", got, tt.want)
			}
			t.Logf("got %v\n", got)
			t.Logf("want %v\n", tt.want)
		})
	}
}
