package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateOffsetsData(t *testing.T) {
	tests := []struct {
		name         string
		startOffsets map[int32]int64
		endOffsets   map[int32]int64
		want         []string
		wantLen      int
	}{
		{
			name: "단일 파티션",
			startOffsets: map[int32]int64{
				0: 100,
			},
			endOffsets: map[int32]int64{
				0: 200,
			},
			want:    []string{"0:100:199"},
			wantLen: 1,
		},
		{
			name: "여러 파티션",
			startOffsets: map[int32]int64{
				0: 100,
				1: 50,
				2: 0,
			},
			endOffsets: map[int32]int64{
				0: 200,
				1: 150,
				2: 75,
			},
			wantLen: 3,
		},
		{
			name: "시작과 끝 오프셋이 같은 경우",
			startOffsets: map[int32]int64{
				0: 100,
			},
			endOffsets: map[int32]int64{
				0: 100,
			},
			want:    []string{"0:100:99"},
			wantLen: 1,
		},
		{
			name: "시작 오프셋이 0인 경우",
			startOffsets: map[int32]int64{
				0: 0,
				1: 0,
			},
			endOffsets: map[int32]int64{
				0: 1000,
				1: 500,
			},
			wantLen: 2,
		},
		{
			name: "큰 오프셋 값들",
			startOffsets: map[int32]int64{
				0: 1000000,
				1: 5000000,
			},
			endOffsets: map[int32]int64{
				0: 2000000,
				1: 6000000,
			},
			wantLen: 2,
		},
		{
			name:         "빈 맵",
			startOffsets: map[int32]int64{},
			endOffsets:   map[int32]int64{},
			want:         []string{},
			wantLen:      0,
		},
		{
			name: "endOffsets에 없는 파티션",
			startOffsets: map[int32]int64{
				0: 100,
				1: 200,
			},
			endOffsets: map[int32]int64{
				0: 300,
				// 파티션 1은 endOffsets에 없음
			},
			want:    []string{"0:100:299"},
			wantLen: 1,
		},
		{
			name: "startOffsets에 없는 파티션",
			startOffsets: map[int32]int64{
				0: 100,
			},
			endOffsets: map[int32]int64{
				0: 300,
				1: 400, // 파티션 1은 startOffsets에 없음
			},
			want:    []string{"0:100:299"},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := createOffsetsData(tt.startOffsets, tt.endOffsets)

			assert.Len(t, result, tt.wantLen)

			if tt.want != nil {
				// 특정 결과가 기대되는 경우
				if tt.wantLen == 1 {
					assert.Contains(t, result, tt.want[0])
				} else if tt.wantLen == 0 {
					assert.Empty(t, result)
				}
			}

			// 각 오프셋 문자열의 형식 검증
			for _, offsetStr := range result {
				assert.Contains(t, offsetStr, ":")
				// 형식: "partition:start:end"
				parts := len(offsetStr)
				assert.Greater(t, parts, 4) // 최소한 "0:0:0" 형태
			}
		})
	}
}

func TestCreateOffsetsData_DetailedValidation(t *testing.T) {
	t.Run("오프셋 계산 검증", func(t *testing.T) {
		startOffsets := map[int32]int64{
			0: 100,
			1: 50,
		}
		endOffsets := map[int32]int64{
			0: 200,
			1: 150,
		}

		result := createOffsetsData(startOffsets, endOffsets)

		// 결과에 두 파티션의 데이터가 모두 있어야 함
		assert.Len(t, result, 2)

		// 각 파티션별로 올바른 형식인지 확인
		foundPartitions := make(map[string]bool)
		for _, offsetStr := range result {
			if offsetStr == "0:100:199" {
				foundPartitions["0"] = true
			}
			if offsetStr == "1:50:149" {
				foundPartitions["1"] = true
			}
		}

		assert.True(t, foundPartitions["0"], "파티션 0의 오프셋 데이터가 올바르지 않음")
		assert.True(t, foundPartitions["1"], "파티션 1의 오프셋 데이터가 올바르지 않음")
	})

	t.Run("end - 1 계산 검증", func(t *testing.T) {
		startOffsets := map[int32]int64{
			0: 1000,
		}
		endOffsets := map[int32]int64{
			0: 1500,
		}

		result := createOffsetsData(startOffsets, endOffsets)

		assert.Len(t, result, 1)
		assert.Equal(t, "0:1000:1499", result[0])
	})
}

func TestCreateOffsetsData_EdgeCases(t *testing.T) {
	t.Run("매우 큰 파티션 ID", func(t *testing.T) {
		startOffsets := map[int32]int64{
			2147483647: 0, // 최대 int32 값
		}
		endOffsets := map[int32]int64{
			2147483647: 100,
		}

		result := createOffsetsData(startOffsets, endOffsets)

		assert.Len(t, result, 1)
		assert.Equal(t, "2147483647:0:99", result[0])
	})

	t.Run("음수 파티션 ID (이론적으로 불가능하지만 테스트)", func(t *testing.T) {
		startOffsets := map[int32]int64{
			-1: 0,
		}
		endOffsets := map[int32]int64{
			-1: 100,
		}

		result := createOffsetsData(startOffsets, endOffsets)

		assert.Len(t, result, 1)
		assert.Equal(t, "-1:0:99", result[0])
	})

	t.Run("nil 맵들", func(t *testing.T) {
		result := createOffsetsData(nil, nil)
		assert.Empty(t, result)
	})
}
