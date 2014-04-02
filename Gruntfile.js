module.exports = function(grunt) {
    "use strict";

    var js_src = {
        libs: [
            'vendor/jquery.min.js',
            'vendor/bootstrap/transition.js',
            'vendor/bootstrap/alert.js',
            'vendor/bootstrap/button.js',
            'vendor/bootstrap/carousel.js',
            'vendor/bootstrap/collapse.js',
            'vendor/bootstrap/dropdown.js',
            'vendor/bootstrap/modal.js',
            'vendor/bootstrap/tooltip.js',
            'vendor/bootstrap/popover.js',
            'vendor/bootstrap/scrollspy.js',
            'vendor/bootstrap/tab.js',
            'vendor/bootstrap/affix.js',
        ],
    };

    var js_dest = {
        libs: 'js/libs.min.js',
    };

    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),

        uglify: {
            options: {
                sourceMap: true,
                mangle: false,
                preserveComments: 'some',
            },
            build: {
                src: js_src.libs,
                dest: js_dest.libs,
            },
        },

        compass: {
            dist: {
                options: {
                    outputStyle: 'compressed',
                    sassDir: 'scss',
                    cssDir: 'css',
                    noLineComments: true,
                    require: [
                        'bootstrap-sass',
                    ],
                },
            },
        },

        jekyll: {
            doctor: true,
            drafts: true,
        },

        watch: {
            scripts: {
                files: [
                    'vendor/**/*.js',
                    'js/**/*.js',
                ],
                tasks: ['uglify'],
                options: {
                    spawn: false,
                },
            },
            css: {
                files: [
                    'scss/**/*.scss',
                ],
                tasks: ['compass'],
                options: {
                    spawn: false,
                },
            },
            all: {
                files: [
                    '**/*',
                    '!_site/**/*',
                ],
                tasks: ['jekyll'],
                options: {
                    spawn: false,
                },
            },
        },
    });

    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-contrib-compass');
    grunt.loadNpmTasks('grunt-jekyll');
    grunt.registerTask('default', ['uglify', 'compass', 'jekyll']);
};
