#include "indexed_row_table.h"
#include <cstring>

namespace bytedance_db_project {
IndexedRowTable::IndexedRowTable(int32_t index_column) {
  index_column_ = index_column;
}

void IndexedRowTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);
    index_[*(cur_row+index_column_*FIXED_FIELD_LEN)].push_back(row_id);
  }

}

int32_t IndexedRowTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  if(row_id<num_rows_&&col_id<num_cols_){
    int IntFieldIndex=FIXED_FIELD_LEN*(row_id*num_cols_+col_id);
    return *(rows_+IntFieldIndex);
  }
  return 0;
}

void IndexedRowTable::PutIntField(int32_t row_id, int32_t col_id,
                                  int32_t field) {
  // TODO: Implement this!
  if(row_id<num_rows_&&col_id<num_cols_){
    int IntFieldIndex=FIXED_FIELD_LEN*(row_id*num_cols_+col_id);
    int val=*(rows_+IntFieldIndex);
    if(col_id==index_column_){
      if(index_[val].size()==1){
        index_.erase(val);
      }else{
        for(size_t i=0;i<index_[val].size();i++){
          if(index_[val][i]==row_id){
            index_[val].erase(index_[val].begin()+i);
            break;
          } 
        }
      }
      index_[field].push_back(row_id);
    }
    *(rows_+IntFieldIndex)=field;
  }
}

int64_t IndexedRowTable::ColumnSum() {
  // TODO: Implement this!
  int64_t sum=0;
  if(index_column_==0){
    for(auto it=index_.begin();it!=index_.end();it++){
      sum+=it->first*it->second.size();
    } 
  }else{
    for(int row_id=0;row_id<num_rows_;row_id++){
      sum+=GetIntField(row_id,0);
    }
  }
  return sum;
}

int64_t IndexedRowTable::PredicatedColumnSum(int32_t threshold1,
                                             int32_t threshold2) {
  // TODO: Implement this!
  int64_t sum=0;
  if(1==index_column_){
    for(auto it=index_.begin();it!=index_.end();it++){
      if(it->first>threshold1){
        for(int row_id:it->second){
          if(GetIntField(row_id,2)<threshold2){
            sum+=GetIntField(row_id,0);
          }
        }
      }
    } 
  }else if(2==index_column_){
    for(auto it=index_.begin();it!=index_.end();it++){
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

int64_t IndexedRowTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t sum=0;
  if(index_column_==0){
    for(auto it=index_.begin();it!=index_.end();it++){
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

int64_t IndexedRowTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t UpdateTimes=0;
  if(index_column_==0){
    for(auto it=index_.begin();it!=index_.end();it++){
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