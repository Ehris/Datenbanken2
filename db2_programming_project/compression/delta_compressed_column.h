#pragma once

#include <core/compressed_column.hpp>

namespace CoGaDB{


/*!
 *  \brief     This class represents a delta compressed column with type T, is the base class for all compressed typed column classes.
 */
template<class T>
class DeltaCompressedColumn : public CompressedColumn<T>{
public:
    /***************** constructors and destructor *****************/
    DeltaCompressedColumn(const std::string& name, AttributeType db_type);
    virtual ~DeltaCompressedColumn();

    virtual bool insert(const boost::any& new_Value);
    virtual bool insert(const T& new_value);
    template <typename InputIterator>
    bool insert(InputIterator first, InputIterator last);

    virtual bool update(TID tid, const boost::any& new_value);
    virtual bool update(PositionListPtr tid, const boost::any& new_value);

    virtual bool remove(TID tid);
    //assumes tid list is sorted ascending
    virtual bool remove(PositionListPtr tid);
    virtual bool clearContent();

    virtual const boost::any get(TID tid);
    //virtual const boost::any* const getRawData()=0;
    virtual void print() const throw();
    virtual size_t size() const throw();
    virtual unsigned int getSizeinBytes() const throw();

    virtual const ColumnPtr copy() const;

    virtual bool store(const std::string& path);
    virtual bool load(const std::string& path);



    virtual T& operator[](const int index);

    /*! values*/
    std::vector<T> deltaColumn;
    std::string _name;

};


/***************** Start of Implementation Section ******************/


    template<class T>
    DeltaCompressedColumn<T>::DeltaCompressedColumn(const std::string& name, AttributeType db_type) : CompressedColumn<T>(name, db_type), deltaColumn(), _name(name) {

    }

    template<class T>
    DeltaCompressedColumn<T>::~DeltaCompressedColumn(){

    }

    template<class T>
    bool DeltaCompressedColumn<T>::insert(const boost::any&){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return false;
    }

    template<class T>
    bool DeltaCompressedColumn<T>::insert(const T& new_value) {
        if(deltaColumn.size() == 0) {
            deltaColumn.push_back(new_value);
        } else {
            deltaColumn.push_back(new_value - this[deltaColumn.size() - 1]);
        }

//        } else {
//            T lastEntry = deltaColumn[0];
//            for(unsigned int i = 1; i < deltaColumn.size(); i++) {
//                lastEntry += deltaColumn[i];
//            }
//            deltaColumn.push_back(new_value - lastEntry);
//        }

        return true;
    }

    template <typename T>
    template <typename InputIterator>
    bool DeltaCompressedColumn<T>::insert(InputIterator, InputIterator){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return true;
    }

    template<class T>
    const boost::any DeltaCompressedColumn<T>::get(TID){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return boost::any();
    }

    template<class T>
    void DeltaCompressedColumn<T>::print() const throw(){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
    }

    template<class T>
    size_t DeltaCompressedColumn<T>::size() const throw(){
        return deltaColumn.size();
    }

    template<class T>
    const ColumnPtr DeltaCompressedColumn<T>::copy() const{
        return ColumnPtr(new DeltaCompressedColumn<T>(*this));
    }

    template<class T>
    bool DeltaCompressedColumn<T>::update(TID tid, const boost::any& new_value){
        if(tid >= deltaColumn.size()) {
            return false;
        }

        T delta = this[tid] - boost::any_cast<T>(new_value);
        for(unsigned int i = tid; i < deltaColumn.size(); i++) {
            deltaColumn[i] += delta;
        }

        return true;
    }

    template<class T>
    bool DeltaCompressedColumn<T>::update(PositionListPtr, const boost::any&){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return false;
    }

    template<class T>
    bool DeltaCompressedColumn<T>::remove(TID tid){
        if(tid >= deltaColumn.size()) {
            return false;
        }

        T delta = deltaColumn[tid];
        deltaColumn.erase(deltaColumn.begin() + tid);

        for(unsigned int i = tid; i < deltaColumn.size(); i++) {
            deltaColumn[i] += delta;
        }

        return true;
    }

    template<class T>
    bool DeltaCompressedColumn<T>::remove(PositionListPtr){
        // NOT NECESSARY FOR OUR PROGRAMMING TASK (NOT USED BY UNIT TEST).
        return false;
    }

    template<class T>
    bool DeltaCompressedColumn<T>::clearContent(){
        deltaColumn.clear();
        return true;
    }

    template<class T>
    bool DeltaCompressedColumn<T>::store(const std::string& path_){
        // Recreate normal/standard column form?

        std::string path(path_);
        path += _name;

//        std::cout << "Writing Column '" << _name << "' to File '" << path << "'" << std::endl;

        std::ofstream outfile (path.c_str(), std::fstream::out | std::fstream::binary);
        boost::archive::binary_oarchive oa(outfile);

//        oa << columnEntries;

        // REMAPS TO THE INITIAL VALUES. PLACEHOLDER UNTIL REAL REQUIREMENTS FOR THIS METHOD ARE CLEAR TO US.
        std::vector<T> simpleColumnRepresentation;
        for(unsigned int i = 0; i < deltaColumn.size(); i++) {
            simpleColumnRepresentation.push_back(this[i]);
        }
        oa << simpleColumnRepresentation;

        outfile.flush();
        outfile.close();

//        for(unsigned int i = 0; i < simpleColumnRepresentation.size(); i++) {
//            std::cout << "Column entry[" << i << "]: " << simpleColumnRepresentation[i] << std::endl;
//        }

        return true;
    }

    template<class T>
    bool DeltaCompressedColumn<T>::load(const std::string& path_){
        std::string path(path_);
        path += _name;

        std::ifstream infile (path.c_str(), std::fstream::in | std::fstream::binary);
        boost::archive::binary_iarchive ia(infile);

//        ia >> columnEntries;

        infile.close();

        return true;
    }

    template<class T>
    T& DeltaCompressedColumn<T>::operator[](const int index){
        T entry = deltaColumn[0];
        for(int i = 1; i <= index; i++) {
            entry += deltaColumn[i];
        }
        return entry;
    }

    template<class T>
    unsigned int DeltaCompressedColumn<T>::getSizeinBytes() const throw(){
        // What shall be returned? Size of columnEntries and dictionary?
        return deltaColumn.capacity() * sizeof(T);
    }

/***************** End of Implementation Section ******************/



}; //end namespace CogaDB

