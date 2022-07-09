#include "custom_table.h"
#include <cstring>
#include<malloc.h>

namespace bytedance_db_project {
CustomTable::CustomTable() {}

CustomTable::~CustomTable() {}

void CustomTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  if(num_cols_*num_rows_<=1000){
    for (auto row_id = 0; row_id < num_rows_; row_id++) {
      auto cur_row = rows.at(row_id);
      std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                  FIXED_FIELD_LEN * num_cols_);
    }
    for(int i=0;i<3;i++) index_columns_[i]=-1;
  }else{
    indexs_.resize(3);
    for (auto row_id = 0; row_id < num_rows_; row_id++) {
      auto cur_row = rows.at(row_id);
      std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                  FIXED_FIELD_LEN * num_cols_);
      CreateIndex(cur_row,row_id);
    }
  }
}

void CustomTable::CreateIndex(char* cur_row,int32_t row_id){
  int sum=row_id*num_cols_;
  for(int i=0;i<3;i++){
    if(index_columns_.at(i)!=-1){
      if(indexs_.at(i).size()&&sum/indexs_.at(i).size()<0.7){
        std::unordered_map<int32_t, std::vector<int32_t>>().swap(indexs_.at(i));
        malloc_trim(0);
        index_columns_.at(i)=-1;
      }else indexs_.at(i)[*(cur_row+index_columns_.at(i)*FIXED_FIELD_LEN)].push_back(row_id);
    }
  }
}

int32_t CustomTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  if(row_id<num_rows_&&col_id<num_cols_){
    int IntFieldIndex=FIXED_FIELD_LEN*(row_id*num_cols_+col_id);
    return *(rows_+IntFieldIndex);
  }
  return 0;
}

void CustomTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  if(row_id<num_rows_&&col_id<num_cols_){
    int IntFieldIndex=FIXED_FIELD_LEN*(row_id*num_cols_+col_id);
    int val=*(rows_+IntFieldIndex);
    for(int i=0;i<3;i++){
        if(col_id==index_columns_[i]){
        if(indexs_[i][val].size()==1){
          indexs_[i].erase(val);
        }else{
          for(size_t j=0;j<indexs_[i][val].size();j++){
            if(indexs_[i][val][j]==row_id){
              indexs_[i][val].erase(indexs_[i][val].begin()+j);
              break;
            } 
          }
        }
        indexs_[i][field].push_back(row_id);
      }
    }
    
    *(rows_+IntFieldIndex)=field;
  }
}

int64_t CustomTable::ColumnSum() {
  // TODO: Implement this!
  int64_t sum=0;
  if(index_columns_[0]==0){
    for(auto it=indexs_[0].begin();it!=indexs_[0].end();it++){
      sum+=it->first*it->second.size();
    } 
  }else{
    for(int row_id=0;row_id<num_rows_;row_id++){
      sum+=GetIntField(row_id,0);
    }
  }
  return sum;
}

int64_t CustomTable::PredicatedColumnSum(int32_t threshold1,
                                         int32_t threshold2) {
  // TODO: Implement this!
  int64_t sum=0;
  if(index_columns_[1]==1){
    for(auto it=indexs_[1].begin();it!=indexs_[1].end();it++){
      if(it->first>threshold1){
        for(int row_id:it->second){
          if(GetIntField(row_id,2)<threshold2){
            sum+=GetIntField(row_id,0);
          }
        }
      }
    } 
  }else if(index_columns_[2]==2){
    for(auto it=indexs_[2].begin();it!=indexs_[2].end();it++){
      if(it->first>threshold2){
        for(int row_id:it->second){
          if(GetIntField(row_id,2)<threshold1){
            sum+=GetIntField(row_id,0);
          }
        }
      }
    } 
  }else{
    for(int row_id=0;row_id<num_rows_;row_id++){
      if(GetIntField(row_id,1)>threshold1&&GetIntField(row_id,2)<threshold2){
        sum+=GetIntField(row_id,0);
      }
    }
  }

  return sum;
}

int64_t CustomTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t sum=0;
  if(index_columns_[0]==0){
    for(auto it=indexs_[0].begin();it!=indexs_[0].end();it++){
      if(it->first>threshold){
        for(int row_id:it->second){
          for(int col_id=0;col_id<num_cols_;col_id++){
            sum+=GetIntField(row_id,col_id);
          }
        }
      }
    } 
  }else{
    for(int row_id=0;row_id<num_rows_;row_id++){
      if(GetIntField(row_id,0)>threshold){
        for(int col_id=0;col_id<num_cols_;col_id++){
          sum+=GetIntField(row_id,col_id);
        }
      }
    }
  }
  return sum;
}
int64_t CustomTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t UpdateTimes=0;
  if(index_columns_[0]==0){
    for(auto it=indexs_[0].begin();it!=indexs_[0].end();it++){
      if(it->first<threshold){
        for(int row_id:it->second){
          PutIntField(row_id,3,GetIntField(row_id,2)+GetIntField(row_id,3));
          UpdateTimes++;
        }
      }
    }
  }else{
    for(int row_id=0;row_id<num_rows_;row_id++){
      if(GetIntField(row_id,0)<threshold){
        PutIntField(row_id,3,GetIntField(row_id,2)+GetIntField(row_id,3));
        UpdateTimes++;
      }
    }
  }
  return UpdateTimes;
}
} // namespace bytedance_db_project