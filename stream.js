var R = require ( 'ramda' );
var H = require ( 'highland' );
var W = require ( 'highland-wrapcallback' );
var A = require ( 'aws-sdk' );

module.exports = R.curry ( function ( s3Parms, listObjectParms ) {
    var S = new A.S3 ( s3Parms );

    var objectStream = function ( listObjectParms ) {
        return H ( function ( push, next ) {
            W ( S, 'listObjects' )( listObjectParms )
                .errors ( R.unary ( push ) )
                .each ( function ( output ) {
                    for ( var i = 0; i < output.Contents.length; i++ ) {
                        push ( null, output.Contents[i] );
                    }

                    if ( output.IsTruncated ) {
                        return next ( objectStream ( R.merge ( listObjectParms, R.compose ( R.createMapEntry ( 'Marker' ), R.prop ( 'Key' ), R.last, R.prop ( 'Contents' ) )( output ) ) ) );
                    }

                    return push ( null, H.nil );
                } );
        } );
    };

    return objectStream ( listObjectParms );
} );
